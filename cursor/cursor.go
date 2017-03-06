/*
Copyright 2016 Google Inc. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package cursor provides facilities to parse Oracle cursors.
package cursor

// Cursor holds performance stats mined from a trace file.
type Cursor struct {
	CursorID       int64
	SQLID          string
	BusinessTxName string
	ELAThreshold   int64
	hashValue      string
	length         int
	depth          int
	uID            int
	lID            int
	oct            int
}

// NewCursor opens a new cursor for PARSING IN CURSOR trace record.
// hashValue represents SQL hash value. length is SQL statement text's length in bytes.
// depth is recursive call depth. uID is parsing user's identity.
// lID is parsing schema's identity. oct is Oracle's command type.
func NewCursor(cursorID int64, SQLID string, businessTxName string, elaThreshold int64, hashValue string, length int, depth int, uID int, lID int, oct int) *Cursor {
	return &Cursor{
		CursorID:       cursorID,
		SQLID:          SQLID,
		BusinessTxName: businessTxName,
		ELAThreshold:   elaThreshold,
		hashValue:      hashValue,
		length:         length,
		depth:          depth,
		uID:            uID,
		lID:            lID,
		oct:            oct,
	}
}
