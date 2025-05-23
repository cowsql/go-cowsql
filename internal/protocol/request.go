package protocol

// DO NOT EDIT
//
// This file was generated by ./schema.sh

// EncodeLeader encodes a Leader request.
func EncodeLeader(request *Message) {
	request.reset()
	request.putUint64(0)

	request.putHeader(RequestLeader, 0)
}

// EncodeClient encodes a Client request.
func EncodeClient(request *Message, id uint64) {
	request.reset()
	request.putUint64(id)

	request.putHeader(RequestClient, 0)
}

// EncodeHeartbeat encodes a Heartbeat request.
func EncodeHeartbeat(request *Message, timestamp uint64) {
	request.reset()
	request.putUint64(timestamp)

	request.putHeader(RequestHeartbeat, 0)
}

// EncodeOpen encodes a Open request.
func EncodeOpen(request *Message, name string, flags uint64, vfs string) {
	request.reset()
	request.putString(name)
	request.putUint64(flags)
	request.putString(vfs)

	request.putHeader(RequestOpen, 0)
}

// EncodePrepare encodes a Prepare request.
func EncodePrepare(request *Message, db uint64, sql string) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)

	request.putHeader(RequestPrepare, 0)
}

// EncodeExecV0 encodes a Exec request.
func EncodeExecV0(request *Message, db uint32, stmt uint32, values NamedValues) {
	request.reset()
	request.putUint32(db)
	request.putUint32(stmt)
	request.putNamedValues(values)

	request.putHeader(RequestExec, 0)
}

// EncodeExecV1 encodes a Exec request.
func EncodeExecV1(request *Message, db uint32, stmt uint32, values NamedValues32) {
	request.reset()
	request.putUint32(db)
	request.putUint32(stmt)
	request.putNamedValues32(values)

	request.putHeader(RequestExec, 1)
}

// EncodeQueryV0 encodes a Query request.
func EncodeQueryV0(request *Message, db uint32, stmt uint32, values NamedValues) {
	request.reset()
	request.putUint32(db)
	request.putUint32(stmt)
	request.putNamedValues(values)

	request.putHeader(RequestQuery, 0)
}

// EncodeQueryV1 encodes a Query request.
func EncodeQueryV1(request *Message, db uint32, stmt uint32, values NamedValues32) {
	request.reset()
	request.putUint32(db)
	request.putUint32(stmt)
	request.putNamedValues32(values)

	request.putHeader(RequestQuery, 1)
}

// EncodeFinalize encodes a Finalize request.
func EncodeFinalize(request *Message, db uint32, stmt uint32) {
	request.reset()
	request.putUint32(db)
	request.putUint32(stmt)

	request.putHeader(RequestFinalize, 0)
}

// EncodeExecSQLV0 encodes a ExecSQL request.
func EncodeExecSQLV0(request *Message, db uint64, sql string, values NamedValues) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)
	request.putNamedValues(values)

	request.putHeader(RequestExecSQL, 0)
}

// EncodeExecSQLV1 encodes a ExecSQL request.
func EncodeExecSQLV1(request *Message, db uint64, sql string, values NamedValues32) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)
	request.putNamedValues32(values)

	request.putHeader(RequestExecSQL, 1)
}

// EncodeQuerySQLV0 encodes a QuerySQL request.
func EncodeQuerySQLV0(request *Message, db uint64, sql string, values NamedValues) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)
	request.putNamedValues(values)

	request.putHeader(RequestQuerySQL, 0)
}

// EncodeQuerySQLV1 encodes a QuerySQL request.
func EncodeQuerySQLV1(request *Message, db uint64, sql string, values NamedValues32) {
	request.reset()
	request.putUint64(db)
	request.putString(sql)
	request.putNamedValues32(values)

	request.putHeader(RequestQuerySQL, 1)
}

// EncodeInterrupt encodes a Interrupt request.
func EncodeInterrupt(request *Message, db uint64) {
	request.reset()
	request.putUint64(db)

	request.putHeader(RequestInterrupt, 0)
}

// EncodeAdd encodes a Add request.
func EncodeAdd(request *Message, id uint64, address string) {
	request.reset()
	request.putUint64(id)
	request.putString(address)

	request.putHeader(RequestAdd, 0)
}

// EncodeAssign encodes a Assign request.
func EncodeAssign(request *Message, id uint64, role uint64) {
	request.reset()
	request.putUint64(id)
	request.putUint64(role)

	request.putHeader(RequestAssign, 0)
}

// EncodeRemove encodes a Remove request.
func EncodeRemove(request *Message, id uint64) {
	request.reset()
	request.putUint64(id)

	request.putHeader(RequestRemove, 0)
}

// EncodeDump encodes a Dump request.
func EncodeDump(request *Message, name string) {
	request.reset()
	request.putString(name)

	request.putHeader(RequestDump, 0)
}

// EncodeCluster encodes a Cluster request.
func EncodeCluster(request *Message, format uint64) {
	request.reset()
	request.putUint64(format)

	request.putHeader(RequestCluster, 0)
}

// EncodeTransfer encodes a Transfer request.
func EncodeTransfer(request *Message, id uint64) {
	request.reset()
	request.putUint64(id)

	request.putHeader(RequestTransfer, 0)
}

// EncodeDescribe encodes a Describe request.
func EncodeDescribe(request *Message, format uint64) {
	request.reset()
	request.putUint64(format)

	request.putHeader(RequestDescribe, 0)
}

// EncodeWeight encodes a Weight request.
func EncodeWeight(request *Message, weight uint64) {
	request.reset()
	request.putUint64(weight)

	request.putHeader(RequestWeight, 0)
}
