package selector

import (
	"strings"
	"testing"

	"typetype-downloader-go/internal/typetype"
)

func TestSelectRequestedCodecIgnoresProfileCase(t *testing.T) {
	for _, codec := range []string{"avc1.4D401E", "av01.0.01M.08"} {
		t.Run(codec, func(t *testing.T) {
			audioCodec := "mp4a.40.2"
			stream := &typetype.StreamResponse{
				VideoOnlyStreams: []typetype.VideoStreamItem{
					{Itag: 134, Height: 360, MimeType: "video/mp4", Codec: &codec,
						DeliveryMethod: "sabr", ManifestURL: "/sabr/manifest/video"},
				},
				AudioStreams: []typetype.AudioStreamItem{
					{Itag: 140, MimeType: "audio/mp4", Codec: &audioCodec,
						DeliveryMethod: "sabr", ManifestURL: "/sabr/manifest/video"},
				},
			}
			selection, err := SelectMP4WithOptions(stream, Options{
				VideoItag: 134, MaxHeight: 360, VideoCodecPrefix: strings.ToLower(codec),
			})
			if err != nil {
				t.Fatal(err)
			}
			if selection.Video.Itag != 134 || *selection.Video.Codec != codec {
				t.Fatalf("selected a different format: %+v", selection.Video)
			}
		})
	}
}

func TestCodecMatchStillRejectsDifferentProfile(t *testing.T) {
	if codecMatches("avc1.4D401E", "avc1.640028") {
		t.Fatal("different profiles must not match")
	}
}
