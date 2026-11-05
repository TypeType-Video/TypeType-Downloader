package sabr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

func consumeDownloadStream(
	reader io.Reader,
	tracks []streamTrack,
	progress byteCounter,
	writeInitialization bool,
) error {
	var magic [downloadMagicSize]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return fmt.Errorf("read SABR stream magic: %w", err)
	}
	if !bytes.Equal(magic[:], downloadMagic) {
		return errors.New("invalid SABR download stream magic")
	}
	copyBuffer := downloadBufferPool.Get().([]byte)
	defer downloadBufferPool.Put(copyBuffer)
	var rawHeader [frameHeaderSize]byte
	for {
		if _, err := io.ReadFull(reader, rawHeader[:]); err != nil {
			return fmt.Errorf("read SABR frame header: %w", err)
		}
		header, err := decodeFrameHeader(rawHeader[:])
		if err != nil {
			return err
		}
		if header.kind == frameComplete {
			if err := validateComplete(tracks, header); err != nil {
				return err
			}
			var trailing [1]byte
			if count, err := reader.Read(trailing[:]); count != 0 || err != io.EOF {
				return errors.New("SABR download stream has trailing data")
			}
			return nil
		}
		track := findTrack(tracks, header.itag)
		if track == nil {
			return fmt.Errorf("SABR frame references unselected itag %d", header.itag)
		}
		if err := consumeFrame(reader, track, header, copyBuffer, progress, writeInitialization); err != nil {
			return err
		}
	}
}

func consumeFrame(
	reader io.Reader,
	track *streamTrack,
	header frameHeader,
	buffer []byte,
	progress byteCounter,
	writeInitialization bool,
) error {
	output := io.Writer(track.output)
	countProgress := true
	switch header.kind {
	case frameInitialization:
		if track.initialized || header.sequence != 0 {
			return fmt.Errorf("invalid SABR initialization for itag %d", track.itag)
		}
		track.initialized = true
		if !writeInitialization {
			output = io.Discard
			countProgress = false
		}
	case frameMedia:
		if track.nextSequence == 0 {
			track.nextSequence = header.sequence
		}
		if !track.initialized || header.sequence <= 0 || header.sequence != track.nextSequence {
			return fmt.Errorf(
				"out-of-order SABR media for itag %d: got %d, want %d",
				track.itag,
				header.sequence,
				track.nextSequence,
			)
		}
		track.nextSequence++
		track.mediaWritten = true
	default:
		return fmt.Errorf("unknown SABR frame type %d", header.kind)
	}
	written, err := copyFrame(output, reader, header.length, buffer)
	if err != nil {
		return fmt.Errorf("write SABR %s track: %w", track.kind, err)
	}
	if written != header.length {
		return io.ErrUnexpectedEOF
	}
	if countProgress {
		progress.add(written)
	}
	return nil
}

func copyFrame(output io.Writer, reader io.Reader, size int64, buffer []byte) (int64, error) {
	var written int64
	for written < size {
		chunk := buffer
		if remaining := size - written; remaining < int64(len(chunk)) {
			chunk = chunk[:remaining]
		}
		if _, err := io.ReadFull(reader, chunk); err != nil {
			return written, err
		}
		count, err := output.Write(chunk)
		written += int64(count)
		if err != nil {
			return written, err
		}
		if count != len(chunk) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func decodeFrameHeader(raw []byte) (frameHeader, error) {
	header := frameHeader{
		kind:     raw[0],
		itag:     int(int32(binary.BigEndian.Uint32(raw[1:5]))),
		sequence: int(int32(binary.BigEndian.Uint32(raw[5:9]))),
		length:   int64(binary.BigEndian.Uint64(raw[9:17])),
	}
	if header.length < 0 || header.length > maxFrameBytes {
		return frameHeader{}, fmt.Errorf("invalid SABR frame length %d", header.length)
	}
	return header, nil
}

func validateComplete(tracks []streamTrack, header frameHeader) error {
	if header.itag != 0 || header.sequence != 0 || header.length != 0 {
		return errors.New("invalid SABR completion frame")
	}
	for index := range tracks {
		if !tracks[index].initialized || !tracks[index].mediaWritten {
			return fmt.Errorf("incomplete SABR %s track", tracks[index].kind)
		}
	}
	return nil
}

func findTrack(tracks []streamTrack, itag int) *streamTrack {
	for index := range tracks {
		if tracks[index].itag == itag {
			return &tracks[index]
		}
	}
	return nil
}

type frameHeader struct {
	kind     byte
	itag     int
	sequence int
	length   int64
}

const (
	downloadMediaType    = "application/vnd.typetype.sabr-download"
	frameInitialization  = 1
	frameMedia           = 2
	frameComplete        = 3
	frameHeaderSize      = 17
	downloadMagicSize    = 8
	streamCopyBufferSize = 256 * 1024
	maxFrameBytes        = 64 << 20
)

var downloadMagic = []byte("TTSABR1\n")

var downloadBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, streamCopyBufferSize)
	},
}
