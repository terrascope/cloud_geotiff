package cloud_geotiff

import (
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

type GeoTiff struct {
	ctx    context.Context
	obj    *storage.ObjectHandle
	buf    []byte
	offset int
}

func NewGeoTiff(bucketName, objectName string) (*GeoTiff, error) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	obj := bucket.Object(objectName)

	return &GeoTiff{
		ctx:    ctx,
		obj:    obj,
		buf:    make([]byte, 1024),
		offset: -1,
	}, nil
}

func (ra *GeoTiff) ReadAt(b []byte, off int64) (int, error) {
	if ra == nil {
		return 0, fmt.Errorf("invalid")
	}

	s := int(off) - ra.offset
	e := (int(off) + len(b)) - ra.offset
	if ra.offset >= 0 && s >= 0 && e <= len(ra.buf) {
		copy(b, ra.buf[s:e])
		return e - s, nil
	}

	if len(b) < len(ra.buf) {
		rc, err := ra.obj.NewRangeReader(ra.ctx, off, int64(len(ra.buf)))
		if err != nil {
			return 0, err
		}
		defer rc.Close()

		_, err = io.ReadFull(rc, ra.buf)
		if err != nil {
			return 0, err
		}
		ra.offset = int(off)
		copy(b, ra.buf[:len(b)])

		return len(b), err

	}

	rc, err := ra.obj.NewRangeReader(ra.ctx, off, int64(len(b)))
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	n, err := io.ReadFull(rc, b)
	return n, err
}

func (ra *GeoTiff) Read(b []byte) (int, error) {
	if ra == nil {
		return 0, fmt.Errorf("invalid")
	}

	rc, err := ra.obj.NewReader(ra.ctx)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	return rc.Read(b)
}
