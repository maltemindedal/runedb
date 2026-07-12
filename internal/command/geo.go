package command

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/maltemindedal/runedb/internal/protocol"
	"github.com/maltemindedal/runedb/internal/storage"
)

// Geospatial commands encode longitude/latitude pairs into 52-bit interleaved
// geohash scores stored in regular sorted sets, matching the Redis layout.
const (
	geoLongitudeMin = -180.0
	geoLongitudeMax = 180.0
	geoLatitudeMin  = -85.05112878
	geoLatitudeMax  = 85.05112878

	// geoStep is the number of bits used per coordinate.
	geoStep = 26

	geoEarthRadiusMeters = 6372797.560856

	geoMetersPerKilometer = 1000.0
	geoMetersPerMile      = 1609.34
	geoMetersPerFoot      = 0.3048
)

func (e *Executor) handleGeoAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 4 || (len(request.Args)-1)%3 != 0 {
		return nil, wrongNumberOfArgumentsError("GEOADD")
	}

	entries := make([]storage.ZSetEntry, 0, (len(request.Args)-1)/3)
	for i := 1; i < len(request.Args); i += 3 {
		longitude, latitude, err := parseGeoCoordinates(request.Args[i], request.Args[i+1])
		if err != nil {
			return nil, err
		}

		entries = append(entries, storage.ZSetEntry{
			Member: request.Args[i+2],
			Score:  float64(geohashEncode(longitude, latitude)),
		})
	}

	key := string(request.Args[0])
	added, evicted, err := e.store.ZAddWithEviction(key, entries)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleGeoDist(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 3 && len(request.Args) != 4 {
		return nil, wrongNumberOfArgumentsError("GEODIST")
	}

	unitMeters := 1.0
	if len(request.Args) == 4 {
		var err error
		unitMeters, err = parseGeoUnitArgument(request.Args[3])
		if err != nil {
			return nil, err
		}
	}

	key := string(request.Args[0])
	scores, found, err := e.store.ZScores(key, [][]byte{request.Args[1], request.Args[2]})
	if err != nil {
		return nil, storageCommandError(err)
	}
	if !found[0] || !found[1] {
		return protocol.BulkString{Null: true}, nil
	}

	firstLongitude, firstLatitude := geohashDecode(geohashBits(scores[0]))
	secondLongitude, secondLatitude := geohashDecode(geohashBits(scores[1]))
	distance := geoDistanceMeters(firstLongitude, firstLatitude, secondLongitude, secondLatitude) / unitMeters

	return protocol.TextBulkString{Value: strconv.FormatFloat(distance, 'f', 4, 64)}, nil
}

func (e *Executor) handleGeoRadius(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 5 {
		return nil, wrongNumberOfArgumentsError("GEORADIUS")
	}
	if len(request.Args) > 5 {
		return nil, ErrSyntaxError()
	}

	longitude, latitude, err := parseGeoCoordinates(request.Args[1], request.Args[2])
	if err != nil {
		return nil, err
	}
	radius, err := parseGeoRadiusArgument(request.Args[3])
	if err != nil {
		return nil, err
	}
	unitMeters, err := parseGeoUnitArgument(request.Args[4])
	if err != nil {
		return nil, err
	}
	radiusMeters := radius * unitMeters

	key := string(request.Args[0])
	entries, err := e.store.ZRange(key, 0, -1)
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(entries))
	for _, entry := range entries {
		memberLongitude, memberLatitude := geohashDecode(geohashBits(entry.Score))
		if geoDistanceMeters(longitude, latitude, memberLongitude, memberLatitude) <= radiusMeters {
			elements = append(elements, protocol.TextBulkString{Value: entry.Member})
		}
	}

	return protocol.Array{Elements: elements}, nil
}

func parseGeoCoordinates(rawLongitude, rawLatitude []byte) (float64, float64, error) {
	longitude, err := parseFloatArgument(rawLongitude)
	if err != nil {
		return 0, 0, err
	}
	latitude, err := parseFloatArgument(rawLatitude)
	if err != nil {
		return 0, 0, err
	}
	if longitude < geoLongitudeMin || longitude > geoLongitudeMax ||
		latitude < geoLatitudeMin || latitude > geoLatitudeMax {
		return 0, 0, newRESPMessageError("ERR", fmt.Sprintf("invalid longitude,latitude pair %f,%f", longitude, latitude))
	}

	return longitude, latitude, nil
}

func parseGeoRadiusArgument(raw []byte) (float64, error) {
	radius, err := parseFloatArgument(raw)
	if err != nil {
		return 0, err
	}
	if radius < 0 {
		return 0, newRESPMessageError("ERR", "radius cannot be negative")
	}

	return radius, nil
}

func parseGeoUnitArgument(raw []byte) (float64, error) {
	switch strings.ToLower(string(raw)) {
	case "m":
		return 1, nil
	case "km":
		return geoMetersPerKilometer, nil
	case "mi":
		return geoMetersPerMile, nil
	case "ft":
		return geoMetersPerFoot, nil
	default:
		return 0, newRESPMessageError("ERR", "unsupported unit provided. please use m, km, ft, mi")
	}
}

func geohashEncode(longitude, latitude float64) uint64 {
	longitudeOffset := (longitude - geoLongitudeMin) / (geoLongitudeMax - geoLongitudeMin)
	latitudeOffset := (latitude - geoLatitudeMin) / (geoLatitudeMax - geoLatitudeMin)

	scale := float64(uint64(1) << geoStep)
	return interleave64(uint32(latitudeOffset*scale), uint32(longitudeOffset*scale))
}

// geohashBits converts a sorted-set score to geohash bits. Scores outside the
// encodable range (written by plain ZADD on the same key) map to zero, because
// Go's out-of-range float-to-integer conversion is platform-dependent.
func geohashBits(score float64) uint64 {
	if score < 0 || score >= float64(uint64(1)<<(2*geoStep+2)) {
		return 0
	}
	return uint64(score)
}

func geohashDecode(bits uint64) (longitude, latitude float64) {
	latitudeBits, longitudeBits := deinterleave64(bits)

	scale := float64(uint64(1) << geoStep)
	latitudeMin := geoLatitudeMin + (float64(latitudeBits)/scale)*(geoLatitudeMax-geoLatitudeMin)
	latitudeMax := geoLatitudeMin + (float64(latitudeBits+1)/scale)*(geoLatitudeMax-geoLatitudeMin)
	longitudeMin := geoLongitudeMin + (float64(longitudeBits)/scale)*(geoLongitudeMax-geoLongitudeMin)
	longitudeMax := geoLongitudeMin + (float64(longitudeBits+1)/scale)*(geoLongitudeMax-geoLongitudeMin)

	return (longitudeMin + longitudeMax) / 2, (latitudeMin + latitudeMax) / 2
}

// interleave64 spreads the bits of x into even positions and y into odd
// positions, producing the Morton code used as the sorted-set score.
func interleave64(x, y uint32) uint64 {
	even := spreadBits(x)
	odd := spreadBits(y)
	return even | (odd << 1)
}

func deinterleave64(interleaved uint64) (x, y uint32) {
	return squashBits(interleaved), squashBits(interleaved >> 1)
}

func spreadBits(value uint32) uint64 {
	spread := uint64(value)
	spread = (spread | (spread << 16)) & 0x0000FFFF0000FFFF
	spread = (spread | (spread << 8)) & 0x00FF00FF00FF00FF
	spread = (spread | (spread << 4)) & 0x0F0F0F0F0F0F0F0F
	spread = (spread | (spread << 2)) & 0x3333333333333333
	spread = (spread | (spread << 1)) & 0x5555555555555555
	return spread
}

func squashBits(spread uint64) uint32 {
	value := spread & 0x5555555555555555
	value = (value | (value >> 1)) & 0x3333333333333333
	value = (value | (value >> 2)) & 0x0F0F0F0F0F0F0F0F
	value = (value | (value >> 4)) & 0x00FF00FF00FF00FF
	value = (value | (value >> 8)) & 0x0000FFFF0000FFFF
	value = (value | (value >> 16)) & 0x00000000FFFFFFFF
	return uint32(value)
}

func geoDistanceMeters(longitude1, latitude1, longitude2, latitude2 float64) float64 {
	longitude1Rad := degreesToRadians(longitude1)
	longitude2Rad := degreesToRadians(longitude2)
	v := math.Sin((longitude2Rad - longitude1Rad) / 2)
	if v == 0 {
		return geoEarthRadiusMeters * math.Abs(degreesToRadians(latitude2)-degreesToRadians(latitude1))
	}

	latitude1Rad := degreesToRadians(latitude1)
	latitude2Rad := degreesToRadians(latitude2)
	u := math.Sin((latitude2Rad - latitude1Rad) / 2)
	a := u*u + math.Cos(latitude1Rad)*math.Cos(latitude2Rad)*v*v

	return 2 * geoEarthRadiusMeters * math.Asin(math.Sqrt(a))
}

func degreesToRadians(degrees float64) float64 {
	return degrees * (math.Pi / 180)
}
