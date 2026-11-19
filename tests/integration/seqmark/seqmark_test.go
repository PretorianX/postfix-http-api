package seqmark

import "testing"

func TestCountUniqueComplete(t *testing.T) {
	text := "x FLOOD-SEQ-00001\nFLOOD-SEQ-00002 y\nFLOOD-SEQ-00001 dup\nFLOOD-SEQ-00003\n"
	count, missing := CountUnique(text, "FLOOD-SEQ-", 5, 3)
	if count != 3 || missing != nil {
		t.Fatalf("CountUnique = %d %v; want 3 nil", count, missing)
	}
}

func TestCountUniqueReportsMissing(t *testing.T) {
	text := "FLOOD-SEQ-00001\nFLOOD-SEQ-00003\n"
	count, missing := CountUnique(text, "FLOOD-SEQ-", 5, 3)
	if count != 2 {
		t.Fatalf("count = %d; want 2", count)
	}
	if len(missing) != 1 || missing[0] != 2 {
		t.Fatalf("missing = %v; want [2]", missing)
	}
}
