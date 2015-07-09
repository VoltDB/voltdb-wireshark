-- This file is part of VoltDB.
-- Copyright (C) 2008-2015 VoltDB Inc.

-- Permission is hereby granted, free of charge, to any person obtaining
-- a copy of this software and associated documentation files (the
-- "Software" ), to deal in the Software without restriction, including
-- without limitation the rights to use, copy, modify, merge, publish,
-- distribute, sublicense, and/or sell copies of the Software, and to
-- permit persons to whom the Software is furnished to do so, subject to
-- the following conditions:

-- The above copyright notice and this permission notice shall be
-- included in all copies or substantial portions of the Software.

-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
-- EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
-- MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
-- IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
-- OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
-- ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
-- OTHER DEALINGS IN THE SOFTWARE.

do
    -- Protocol definition ----------------------------------------------------------------------------
    local p_voltcw = Proto( "VoltCW", "VoltDB Client Wire Protocol" )

    -- Definitions for data fields. -------------------------------------------------------------------
    local f_varLenNode = ProtoField.bytes( "VoltCW.varlennode", "Variable length data node", base.HEX )
    local f_varlen = ProtoField.int32( "VoltCW.varlen", "Length" )
    local f_str = ProtoField.string( "VoltCW.str", "String" )
    local f_varbinary = ProtoField.bytes( "VoltCW.varbinary", "VarBinary", base.HEX )
    local f_int8 = ProtoField.int8( "VoltCW.int8", "TinyInt Value" )    
    local f_int16 = ProtoField.int16( "VoltCW.int16", "SmallInt Value" )
    local f_int32 = ProtoField.int32( "VoltCW.int32", "Integer Value" )
    local f_int64 = ProtoField.int64( "VoltCW.int64", "BigInt Value" )
    local f_float = ProtoField.double( "VoltCW.float", "Float Value" )
    local f_decimal = ProtoField.bytes( "VoltCW.decimal", "Decimal Value" )

    -- VoltDB data types and their sizes in terms of byte.
    local VOLT_TYPE = { [-99] = "ARRAY", [1] = "NULL", [3] = "TINYINT", [4] = "SMALLINT", [5] = "INTEGER",
                        [6] = "BIGINT", [8] = "FLOAT", [9] = "STRING", [11] = "TIMESTAMP", [22] = "DECIMAL",
                        [25] = "VARBINARY" }
    local VOLT_TYPE_LENGTH = { [3] = 1, [4] = 2, [5] = 4, [6] = 8, [8] = 8, [11] = 8, [22] = 16 }
    local VOLT_TYPE_DESC = { [3] = f_int8, [4] = f_int16, [5] = f_int32, [6] = f_int64, [8] = f_float, [11] = f_int64 }

    -- Definitions for the enumerations ---------------------------------------------------------------
    local VALS_BOOL = { [0] = "False", [1] = "True" }
    local VALS_BLANK_BYTE = { [0] = "", [1] = "", [2] = "", [4] = "",
                         [3] = "", [5] = "", [6] = "",  [7] = "" }
    local STATUSCODE = { [1] = "SUCCESS", [-1] = "USER_ABORT", [-2] = "GRACEFUL_FAILURE",
                          [-3] = "UNEXPECTED_FAILURE", [-4] = "CONNECTION_LOST", [-5] = "SERVER_UNAVAILABLE",
                          [-6] = "CONNECTION_TIMEOUT", [-7] = "RESPONSE_UNKNOWN", [-8] = "TXN_RESTART",
                          [-9] = "OPERATIONAL_FAILURE" }
    local AUTHRESult = { [0] = "SUCCESS", [1] = "TOO MANY CONNECTIONS", [2] = "AUTHENTICATION FAILED DUE TO TIMEOUT",
                       [3] = "CORRUPTED OR INVALID LOGIN MESSAGE" }

    -- Definitions for the message. -------------------------------------------------------------------
    local f_msgLen = ProtoField.int32( "VoltCW.msglen", "Message Length" )
    local f_msg = ProtoField.bytes( "VoltCW.msg", "Message", base.HEX )
    local f_version = ProtoField.int8( "VoltCW.version", "Protocol Version" )

    -- Definitions for the Invocation Request fields. -------------------------------------------------
    local f_clientData = ProtoField.uint64( "VoltCW.reqclidata", "Client Data", base.HEX )
    local f_params = ProtoField.bytes( "VoltCW.params", "Parameters", base.HEX )
    local f_paramsCount = ProtoField.int16( "VoltCW.paramscount", "Parameter Count" )
    local f_paramType = ProtoField.int8( "VoltCW.paramtype", "Parameter Type", base.DEC, VOLT_TYPE, 0xFF)
    local f_arrayElementCount = ProtoField.int16( "VoltCW.arrayelementcount", "Element Count" )
    local f_arrayType = ProtoField.int8( "VoltCW.arraytype", "Element Type", base.DEC, VOLT_TYPE, 0xFF)
    local f_param = ProtoField.bytes( "VoltCW.para", "Parameter", base.HEX )

    -- Definitions for the Invocation Response fields. ------------------------------------------------
    local f_fpresent = ProtoField.uint8( "VoltCW.fpresent", "Fields Present Flags", base.HEX, VALS_BLANK_BYTE, 0xE0 )
    -- bit 6 indicates the presence of a status string
    local fb_status = ProtoField.uint8( "VoltCW.fbstatus", "Presence of a status string", base.DEC, VALS_BOOL, 0x20 )
    -- bit 7 indicates the presence of a serializable exception
    local fb_sException = ProtoField.uint8( "VoltCW.fbsException", "Presence of a serializable exception", base.DEC, VALS_BOOL, 0x40 )
    -- bit 8 indicates the presence of an app status string
    local fb_appstatus = ProtoField.uint8( "VoltCW.fbappstatus", "Presence of an app status string", base.DEC, VALS_BOOL, 0x80 )
    local f_statusCode = ProtoField.int8( "VoltCW.statuscode", "Status Code", base.DEC, STATUSCODE, 0xFF )
    local f_appStatus = ProtoField.int8( "VoltCW.appstatus", "Application Status Code" )
    local f_clusterRTT = ProtoField.int32( "VoltCW.clusterrtt", "Cluster Round Trip Time" )
    local f_sExceptionLen = ProtoField.int32( "VoltCW.sexlen", "Serialized Exception Length" )
    local f_sException = ProtoField.bytes( "VoltCW.sexception", "Serialized Exception", base.HEX )
    local f_resultCount = ProtoField.int16( "VoltCW.resultcount", "Result Count" )

    -- Definitions for VoltTable ----------------------------------------------------------------------
    local f_tbl = ProtoField.bytes( "VoltCW.tbl", "Result Table", base.HEX )
    local f_tblLen = ProtoField.int32( "VoltCW.tbllen", "Table Length" )
    local f_metadata = ProtoField.bytes( "VoltCW.metadata", "Table Metadata", base.HEX )
    local f_metadataLen = ProtoField.int32( "VoltCW.metadatalen", "Metadata Length" )
    local f_metaStatus = ProtoField.int8( "VoltCW.metastatus", "Status Code" )
    local f_metaColCount = ProtoField.int16( "VoltCW.metacolcnt", "Column Count" )
    local f_colType = ProtoField.int8( "VoltCW.coltype", "Column Type", base.DEC, VOLT_TYPE, 0xFF )
    local f_rowCount = ProtoField.int32( "VoltCW.rowcount", "Row Count" )
    local f_rows = ProtoField.bytes( "VoltCW.rows", "Data Rows", base.HEX )
    local f_row = ProtoField.bytes( "VoltCW.row", "Row", base.HEX )
    local f_rowLen = ProtoField.int32( "VoltCW.rowlen", "Row Length" )
    local f_col = ProtoField.bytes( "VoltCW.col", "Column", base.HEX )

    -- Definitions for login request ------------------------------------------------------------------
    local f_hashVersion = ProtoField.int8( "VoltCW.hashver", "Password Hash Version" )
    local f_srv = ProtoField.bytes( "VoltCW.srv", "Service" )
    local f_uname = ProtoField.bytes( "VoltCW.uname", "User Name" )
    local f_phash = ProtoField.bytes( "VoltCW.phash", "Password Hash", base.HEX )

    -- Definitions for login response -----------------------------------------------------------------
    local f_authResult = ProtoField.int8( "VoltCW.authres", "Authentication Result", base.DEC, AUTHRESult, 0xFF )
    local f_shid = ProtoField.int32( "VoltCW.shid", "Service Host ID" )
    local f_cid = ProtoField.int64( "VoltCW.cid", "Connection ID" )
    local f_cstart = ProtoField.int64( "VoltCW.cstart", "Cluster Start Timestamp" )
    local f_leader_ipv4 = ProtoField.ipv4( "VoltCW.leaderipv4", "Leader IPv4 Address" )
    local f_bdstring = ProtoField.bytes( "VoltCW.bdstring", "Build String", base.HEX )

    -- Register fields. -------------------------------------------------------------------------------
    p_voltcw.fields = {
                          f_msgLen, f_msg, f_version, -- message header
                          f_varLenNode, f_varlen, f_str, f_varbinary, f_int8, f_int16, f_int32, f_int64, f_float, f_decimal, -- data fields
                          f_clientData, f_params, f_paramsCount, f_paramType, f_arrayType, f_arrayElementCount, f_param, -- invocation request
                          f_invokeRsp, f_fpresent, fb_status, fb_sException, -- invocation response
                          fb_appstatus, f_statusCode, f_appStatus, f_clusterRTT,
                          f_sExceptionLen, f_sException, f_resultCount, 
                          f_tbl, f_tblLen, f_metadata, f_metadataLen, f_metaStatus, f_metaColCount, -- VoltTable
                          f_colType, f_rowCount, f_rows, f_row, f_rowLen, f_col,
                          f_hashVersion, f_srv, f_uname, f_phash, -- login request
                          f_authResult, f_shid, f_cid, f_cstart, f_leader_ipv4, f_bdstring -- login response
                      }

    local msgCount = 0
    local hashasException = false -- indicates if any exception found

    local function checkBuffer(bufLen, offset, value_len)
        return (bufLen - offset >= value_len)
    end

    -- Get next value in the buffer and return:
    ---- the new offset after reading
    ---- the value
    local function nextValue( buf, offset, size )
        -- Check if there is enough room in the buffer for a next value.
        local bufLen = buf:len()
        if offset < 0 or size <= 0 or not checkBuffer(bufLen, offset, size) then return offset, nil end
        local value = buf( offset, size )
        offset = offset + size
        return offset, value
    end

    -- Get next string / varbinary from the buffer.
    -- Return value:
    ---- the new offset after reading
    ---- the value length
    ---- the value
    local function nextLengthPrecededValue( buf, offset )
        -- get value length
        local valueLength
        offset, valueLength = nextValue( buf, offset, 4 )
        if valueLength == nil then return offset, nil, nil end

        -- get value
        local value
        offset, value = nextValue( buf, offset, valueLength:int() )
        return offset, valueLength, value
    end

    -- Get next type preceded value.
    -- Return value:
    --- the new offset after reading
    --- the type of the value
    --- the length of the value (length of valueType + value)
    --- the value itself
    local function nextTypePrecededValue( buf, offset )
        local valueType, valueLength, value
        valueLength = 1

        offset, valueType = nextValue( buf, offset, 1 )
        if valueType == nil then return offset, nil, nil, nil end
        local i_valueType = valueType:int()

        if i_valueType == 9 or i_valueType == 25 then -- string or varbinary
            value = {}
            offset, value[0], value[1] = nextLengthPrecededValue( buf, offset )
            if value[0] == nil then return offset, nil, nil, nil end
            valueLength = valueLength + 4 + value[0]:int()
        elseif i_valueType == 1 then -- null
            value = nil
        elseif i_valueType == -99 then -- array
            -- do nothing for array here.
            return offset-1, valueType, nil, nil
        else
            local typeLength = VOLT_TYPE_LENGTH[ i_valueType ]
            if typeLength == nil then return offset, nil, nil, nil end
            offset, value = nextValue( buf, offset, typeLength )
            if value == nil then return offset, nil, nil, nil end
            valueLength = valueLength + typeLength
        end
        
        return offset, valueType, valueLength, value
    end

    local function getDecimal( binary )
        if binary:len() ~= 16 then return nil end
        high = binary(0, 8):int64()
        low = binary(8, 8):int64()
        local returnValue = ""
        if high > 0 then returnValue = returnValue .. high end
        returnValue = returnValue .. low

        local len = string.len(returnValue)
        if len > 12 then
            returnValue = string.sub(returnValue, 1, len-12) .. "." .. string.sub(returnValue, -12)
        end
        return returnValue
    end

    local function buildStringSubtree(strlen, str, stringNode, infoString)
        string = ( str == nil and "[empty string]" or str:string() )
        stringNode:set_text( infoString .. string )
        stringNode:add( f_varlen, strlen )
        if str == nil then return end
        stringNode:add( f_str, str )
    end

    local function setError( errorMessage, treeNode )
        if not hasException then treeNode:append_text(": " .. errorMessage) end
        hasException = true
    end

    -- Dissector for login requests
    local function decodeLoginRequest( msgBody, pkt, msgSubtree )
        local offset = 0
        local v_hashVersion
        offset, v_hashVersion = nextValue( msgBody, offset, 1 )
        if v_hashVersion == nil then return false end
        local srvStrLen, srvString, unameStrlen, unameString
        -- Get service
        local srvStart = offset
        offset, srvStrLen, srvString = nextLengthPrecededValue( msgBody, offset )
        if srvString == nil then return false end
        
        -- Get user name
        local unameStart = offset
        offset, unameStrlen, unameStr = nextLengthPrecededValue( msgBody, offset )
        if unameStrlen == nil then return false end

        -- Get password hash
        local phash_len
        if v_hashVersion:int() == 0 then
            phash_len = 20
        elseif v_hashVersion:int() == 1 then
            phash_len = 32
        end
        local v_phash
        offset, v_phash = nextValue( msgBody, offset, phash_len )
        if v_phash == nil then return false end

        msgSubtree:add( f_hashVersion, v_hashVersion )
        local srvSubtree = msgSubtree:add( f_srv, msgBody( srvStart, 4 + srvStrLen:int() ) )
        buildStringSubtree( srvStrLen, srvString, srvSubtree, "Service: " )
        local unameSubtree = msgSubtree:add( f_srv, msgBody( unameStart, 4 + unameStrlen:int() ) )
        buildStringSubtree( unameStrlen, unameStr, unameSubtree, "User Name: " )
        msgSubtree:add( f_phash, v_phash )
        pkt.cols.info:set( "VoltDB Login Request")
        msgSubtree:append_text( "Login Request" )
        return true
    end

    -- Dissector for login responses
    local function decodeLoginResponse( msgBody, pkt, msgSubtree )
        local offset = 0
        local v_authResult
        offset, v_authResult = nextValue( msgBody, offset, 1 )
        if v_authResult == nil then return false end
        local v_shid, v_cid, v_cstart, v_ipv4, bd_len, bd_str
        if v_authResult:int() < 0 or v_authResult:int() > 3 then return false end
        if v_authResult:int() == 0 then 
            offset, v_shid = nextValue( msgBody, offset, 4 )
            if v_shid == nil then return false end
            offset, v_cid = nextValue( msgBody, offset, 8 )
            if v_cid == nil then return false end
            offset, v_cstart = nextValue( msgBody, offset, 8 )
            if v_cstart == nil then return false end
            offset, v_ipv4 = nextValue( msgBody, offset, 4 )
            if v_ipv4 == nil then return false end
            offset, bd_len, bd_str = nextLengthPrecededValue( msgBody, offset )
            if bd_str == nil then return false end
        end
        if offset ~= msgBody:len() then return false end
        msgSubtree:add( f_authResult, v_authResult )
        if v_authResult:int() == 0 then 
            msgSubtree:add( f_shid, v_shid )
            msgSubtree:add( f_cid, v_cid )
            msgSubtree:add( f_cstart, v_cstart )
            msgSubtree:add( f_leader_ipv4, v_ipv4 )
            local bdstring_subtree = msgSubtree:add( f_bdstring, msgBody( offset - 4 - bd_len:int(), 4 + bd_len():len() ) )
            bdstring_subtree:set_text( "Build String: " .. bd_str:string() )
            bdstring_subtree:add( f_varlen, bd_len )
            bdstring_subtree:add( f_str, bd_str )
        end
        pkt.cols.info:set( "VoltDB Login Response (" .. AUTHRESult[ v_authResult:int() ] .. ")" )
        msgSubtree:append_text( "Login Response (" .. AUTHRESult[ v_authResult:int() ] .. ")" )
        return true
    end

    local function decodeTableMetadata( buf, metadataSubtree )
        local offset = 0
        local v_metaStatus
        offset, v_metaStatus = nextValue( buf, offset, 1 )
        if v_metaStatus == nil then return end
        metadataSubtree:add( f_metaStatus, v_metaStatus )

        -- Process columns
        offset, v_metaColCount = nextValue( buf, offset, 2 )
        if v_metaColCount == nil then return end
        local i_metaColCount = v_metaColCount:int()
        if i_metaColCount <= 0 then return end

        metadataSubtree:add( f_metaColCount, v_metaColCount )
        local columnTypes = {}
        local columnNames = {}
        for i = 1, i_metaColCount do
            offset, columnTypes[i] = nextValue( buf, offset, 1 )
            if columnTypes[i] == nil then return end
        end
        for i = 1, i_metaColCount do
            columnNames[i] = {}
            offset, columnNames[i][0], columnNames[i][1] = nextLengthPrecededValue( buf, offset )
            if columnNames[i][1] == nil then return end
            if VOLT_TYPE[ columnTypes[i]:int() ] == nil then return end
            local colSubtree = metadataSubtree:add( "Column " .. i .. ": " .. columnNames[i][1]:string() .. " (" .. VOLT_TYPE[ columnTypes[i]:int() ] .. ")")
            colSubtree:set_generated()
            colSubtree:add( f_colType, columnTypes[i] )
            colSubtree:add( f_varlen, columnNames[i][0] ):set_text( "Name Length: " .. columnNames[i][0]:int() )
            colSubtree:add( f_str, columnNames[i][1] ):set_text( "Column Name: " .. columnNames[i][1]:string() )
        end
        return i_metaColCount, columnTypes
    end

    local function decodeVoltTableRows( buf, rowCount, columnCount, columnTypes, rowsSubtree )
        local offset = 0

        local i, j
        for i = 1, rowCount do
            local rowLen, rowData
            offset, rowLen, rowData = nextLengthPrecededValue( buf, offset )
            if rowData == nil then return false end
            local i_rowLen = rowLen:int()
            if i_rowLen <= 0 then return false end
            offset = offset - i_rowLen

            local rowSubtree = rowsSubtree:add( f_row, buf( offset - 4, i_rowLen + 4 ) ):set_text("Row " .. i .. " (" .. i_rowLen .. " bytes)")
            rowSubtree:add( f_rowLen, rowLen ):append_text(" bytes")
            for j = 1, columnCount do
                if columnTypes[j]:int() == 9 then -- string
                    local len, str
                    offset, len, str = nextLengthPrecededValue( buf, offset )
                    if str ~= nil then
                        local strNode = rowSubtree:add( f_col, buf( offset - 4 - len:int(), 4 + len:int() ) )
                        strNode:set_text( "Column " .. j .. " (STRING): " .. str:string() )
                        strNode:add( f_varlen, len )
                        strNode:add( f_str, str )
                    else
                        local strNode = rowSubtree:add( f_col, len )
                        strNode:set_text( "Column " .. j .. " (STRING), length = " .. len:int() )
                    end
                elseif columnTypes[j]:int() == 25 then -- varbinary
                    local len, varbinary
                    offset, len, varbinary = nextLengthPrecededValue( buf, offset )
                    if varbinary ~= nil then
                        local strNode = rowSubtree:add( f_col, buf( offset - 4 - len:int(), 4 + len:int() ) )
                        strNode:set_text( "Column " .. j .. " (VARBINARY) " )
                        strNode:add( f_varlen, len )
                        strNode:add( f_varbinary, varbinary )
                    else
                        local strNode = rowSubtree:add( f_col, len )
                        strNode:set_text( "Column " .. j .. " (VARBINARY), length = " .. len:int() )
                    end
                elseif columnTypes[j]:int() == 3 then -- tinyint
                    local value
                    offset, value = nextValue( buf, offset, 1 )
                    if value ~= nil then
                        rowSubtree:add( f_int8, value ):set_text( "Column " .. j .. " (TINYINT): " .. value:int() )
                    end
                elseif columnTypes[j]:int() == 4 then -- smallint
                    local value
                    offset, value = nextValue( buf, offset, 2 )
                    if value ~= nil then
                        rowSubtree:add( f_int16, value ):set_text( "Column " .. j .. " (SMALLINT): " .. value:int() )
                    end
                elseif columnTypes[j]:int() == 5 then -- integer
                    local value
                    offset, value = nextValue( buf, offset, 4 )
                    if value ~= nil then
                        rowSubtree:add( f_int32, value ):set_text( "Column " .. j .. " (INTEGER): " .. value:int() )
                    end
                elseif columnTypes[j]:int() == 11 then -- timestamp
                    local value
                    offset, value = nextValue( buf, offset, 8 )
                    if value ~= nil then
                        rowSubtree:add( f_int64, value ):set_text( "Column " .. j .. " (TIMESTAMP): " .. value:int64() )
                    end
                elseif columnTypes[j]:int() == 6 then -- bigint
                    local value
                    offset, value = nextValue( buf, offset, 8 )
                    if value ~= nil then
                        rowSubtree:add( f_int64, value ):set_text( "Column " .. j .. " (BIGINT): " .. value:int64() )
                    end
                elseif columnTypes[j]:int() == 8 then -- float
                    local value
                    offset, value = nextValue( buf, offset, 8 )
                    if value ~= nil then
                        rowSubtree:add( f_float, value ):set_text( "Column " .. j .. " (FLOAT): " .. value:float() )
                    end
                elseif columnTypes[j]:int() == 22 then -- decimal
                    local value
                    offset, value = nextValue( buf, offset, 16 )
                    if value ~= nil then
                        rowSubtree:add( f_decimal, value ):set_text( "Column " .. j .. " (DECIMAL): " .. getDecimal(value) )
                    end
                elseif columnTypes[j]:int() == 1 then -- null
                    -- nothing to be done
                end
            end
        end
        return true
    end

    local function decodeVoltTable( buf, tableSubtree )
        local offset = 0
        local bufLen = buf:len()

        -- Get metadata of the VoltTable
        local v_metadataLen, v_metadata
        offset, v_metadataLen, v_metadata = nextLengthPrecededValue( buf, offset )
        if v_metadata == nil then return false end

        tableSubtree:add( f_metadataLen, v_metadataLen ):append_text( " bytes" )
        local metadataSubtree = tableSubtree:add( f_metadata, v_metadata ):set_text( "Table Metadata" )
        local i_metaColCount, columnTypes = decodeTableMetadata( v_metadata, metadataSubtree )
        if columnTypes == nil then return false end

        local v_rowCount
        offset, v_rowCount = nextValue( buf, offset, 4 )
        if v_rowCount == nil then return false end
        tableSubtree:add( f_rowCount, v_rowCount )
        if v_rowCount:int() < 0 then return false end
        if v_rowCount:int() == 0 then return true end

        -- Add rows
        local v_rows
        offset, v_rows = nextValue( buf, offset, bufLen - offset )
        if v_rows == nil then return false end

        local rowsSubtree = tableSubtree:add( f_rows, v_rows ):set_text("Data Rows")
        return decodeVoltTableRows( v_rows, v_rowCount:int(), i_metaColCount, columnTypes, rowsSubtree )
    end

    -- Dissector for invoke responses
    local function decodeInvocationResponse( msgBody, pkt, msgSubtree )
        local bufLen = msgBody:len()
        local offset = 0

        -- Get the client data (8 bytes)
        local v_clientData
        offset, v_clientData = nextValue( msgBody, offset, 8 )
        if v_clientData == nil then return false end

        -- Get the fields present flags (1 byte)
        local v_fpresent
        offset, v_fpresent = nextValue( msgBody, offset, 1 )
        if v_fpresent == nil then return false end
        local bool_status = v_fpresent:bitfield(2)
        local bool_sException = v_fpresent:bitfield(1)
        local bool_appStatus = v_fpresent:bitfield(0)

        -- Get the status code (1 byte)
        local v_statusCode
        offset, v_statusCode = nextValue( msgBody, offset, 1 )
        if v_statusCode == nil then return false end

        -- Get the status string if there is one
        local v_statusStrLen, v_statusString, statusStringStart
        if bool_status == 1 then
            statusStringStart = offset
            offset, v_statusStrLen, v_statusString = nextLengthPrecededValue( msgBody, offset )
            if v_statusStrLen == nil and v_statusString == nil then return false end
        end

        -- Get the application status code
        local v_appStatus
        offset, v_appStatus = nextValue( msgBody, offset, 1 )
        if v_appStatus == nil then return false end

        -- Get the application status string if there is one
        local v_appStatusStrLen, v_appStatusString, appStatusStringStart
        if bool_appStatus == 1 then
            appStatusStringStart = offset
            offset, v_appStatusStrLen, v_appStatusString = nextLengthPrecededValue( msgBody, offset )
            if v_appStatusStrLen == nil and v_appStatusString == nil then return false end
        end

        -- Cluster round-trip time
        local v_clusterRTT
        offset, v_clusterRTT = nextValue( msgBody, offset, 4 )
        if v_clusterRTT == nil then return false end

        -- Get serialized exception count if such exceptions are present
        local v_sExceptionLen, v_sException
        if bool_sException == 1 then
            offset, v_sExceptionLen = nextValue( msgBody, offset, 4 )
            if v_sExceptionLen == nil then return false end
            if v_sExceptionLen:int() <= 0 then return false end
            offset, v_sException = nextValue( msgBody, offset, v_sExceptionLen:int() )
            if v_sException == nil then return false end
        end

        -- Get result count
        local v_resultCount
        offset, v_resultCount = nextValue( msgBody, offset, 2 )
        if v_resultCount == nil then return false end
        local i_resultCount = v_resultCount:int()
        if i_resultCount < 0 then return false end
        local tblLen = {}
        local tbl = {}
        for i = 1, i_resultCount, 1 do
            offset, tblLen[i], tbl[i] = nextLengthPrecededValue( msgBody, offset )
            if tbl[i] == nil then return false end
        end

        -- Now we are confident that the message is an invocation response message. ------------------------
        if STATUSCODE[ v_statusCode:int() ] == nil then return false end
        msgSubtree:append_text( "Invocation Response (" .. STATUSCODE[ v_statusCode:int() ] .. ")" )
        msgSubtree:add( f_clientData, v_clientData )
        -- Add field present information subtree
        local fieldPresentTree = msgSubtree:add( f_fpresent, v_fpresent )
        fieldPresentTree:add( fb_status, v_fpresent )
        fieldPresentTree:add( fb_sException, v_fpresent )
        fieldPresentTree:add( fb_appstatus, v_fpresent )
        -- Status code...
        msgSubtree:add( f_statusCode, v_statusCode )
        if bool_status == 1 then
            local statusNode = msgSubtree:add( f_varLenNode, msgBody( statusStringStart, v_statusStrLen:int() + 4 ) )
            buildStringSubtree( v_statusStrLen, v_statusString, statusNode, "Status String: " )
        end
        -- Application status code...
        msgSubtree:add( f_appStatus, v_appStatus )
        if bool_appStatus == 1 then
            local statusNode = msgSubtree:add( f_varLenNode, msgBody( appStatusStringStart, v_appStatusStrLen:int() + 4 ) )
            buildStringSubtree( v_appStatusStrLen, v_appStatusString, statusNode, "Application Status String: " )
        end
        -- Cluster round-trip time
        msgSubtree:add( f_clusterRTT, v_clusterRTT ):append_text( " ms" )
        -- Add serialized exceptions
        if bool_sException == 1 then
            msgSubtree:add( f_sExceptionLen, v_sExceptionLen )
            msgSubtree:add( f_sException, v_sException )
        end

        -- Add results
        msgSubtree:add( f_resultCount, v_resultCount )
        for i = 1, i_resultCount, 1 do
            local tableSubtree = msgSubtree:add( f_tbl, tbl[i] ):set_text( "Result Table " .. i )
            tableSubtree:add( f_tblLen, tblLen[i] )
            if not decodeVoltTable( tbl[i], tableSubtree ) then
                setError("There was an error when decoding the VoltTable.", tableSubtree)
            end
        end

        pkt.cols.info:set( "VoltDB Invocation Response (" .. STATUSCODE[ v_statusCode:int() ] .. ")" )
        return true
    end

    local function tryDecodeArrayParameter( paramsBuf, offset, parentTreeNode )
        local elementType
        local initialOffset = offset
        offset = offset + 1
        offset, elementType = nextValue(paramsBuf, offset, 1)
        if elementType == nil then return offset, nil end
        local i_elementType = elementType:int()

        offset, elementCount = nextValue(paramsBuf, offset, 2)
        if elementCount == nil then return offset, nil end
        local i_elementCount = elementCount:int()

        local dataLength = VOLT_TYPE_LENGTH[ i_elementType ]
        local typeDescriptor = VOLT_TYPE_DESC[ i_elementType ]
        if i_elementType ~= 9 and i_elementType ~=25 and dataLength == nil then
            return offset, nil
        end

        -- begin to read array data
        local array = {}
        local arrayStart = offset
        for i = 1, i_elementCount do
            if i_elementType == 9 or i_elementType == 25 then -- string or varbinary
                array[i] = {}
                offset, array[i][0], array[i][1] = nextLengthPrecededValue( paramsBuf, offset )
                array[i][2] = offset - array[i][0]:int() - 4 -- start offset
                if array[i][0] == nil then return offset, nil end
            else
                offset, array[i] = nextValue( paramsBuf, offset, dataLength )
                if array[i] == nil then return offset, nil end
            end
        end

        -- begin to build array tree
        local paramTreeNode = parentTreeNode:add( f_param, paramsBuf(initialOffset, offset - initialOffset) )
        paramTreeNode:add( f_paramType, paramsBuf(initialOffset, 1) )
        paramTreeNode:add( f_arrayType, elementType )
        paramTreeNode:add( f_arrayElementCount, elementCount )
        local arraySubtree = paramTreeNode:add( f_param, paramsBuf(arrayStart, offset - arrayStart) )
        arraySubtree:set_text("Array Data")
        for i = 1, i_elementCount do
            if i_elementType == 9 then -- string
                local arrayNode = arraySubtree:add( f_params, paramsBuf(array[i][2], array[i][0]:int() + 4) )
                buildStringSubtree( array[i][0], array[i][1], arrayNode, "" )
            elseif i_elementType == 25 then -- varbinary
                local arrayNode = arraySubtree:add( f_params, paramsBuf(array[i][2], array[i][0]:int() + 4) )
                arrayNode:set_text("VARBINARY element")
                arrayNode:add( f_varlen, array[i][0] )
                arrayNode:add( f_varbinary, array[i][1] )
            else
                if i_paramType == 22 then -- decimal
                    arraySubtree:add( f_decimal, array[i] ) : set_text( getDecimal(array[i]) )
                else
                    arraySubtree:add( typeDescriptor, array[i] )
                end
            end
        end
        return offset, paramTreeNode
    end

    local function buildInvocationRequestParamsTree( paramsCount, paramsBuf, paramSubtree )
        local paramType, paramLength, param
        local offset = 0
        -- Decode parameters
        for i = 1, paramsCount do
            local paramType
            offset, paramType = nextValue( paramsBuf, offset, 1 )
            if paramType == nil then return false end
            offset = offset - 1
            if paramType:int() == -99 then
                local paramTreeNode
                offset, paramTreeNode = tryDecodeArrayParameter( paramsBuf, offset, paramSubtree )
                if paramTreeNode == nil then return false end
                paramTreeNode:set_text( "Parameter " .. i .. " (ARRAY)" )
            else
                offset, paramType, paramLength, param = nextTypePrecededValue( paramsBuf, offset )
                local i_paramType = paramType:int()
                if i_paramType == 9 then -- string
                    local paramTreeNode = paramSubtree:add( f_param, paramsBuf( offset-paramLength, paramLength ) )
                    paramTreeNode:add( f_paramType, paramType )
                    buildStringSubtree( param[0], param[1], paramTreeNode, "Parameter " .. i .. " (STRING): " )
                elseif i_paramType == 25 then -- varbinary
                    local paramTreeNode = paramSubtree:add( f_param, paramsBuf( offset-paramLength, paramLength ) ):set_text( "Parameter " .. i .. " (VARBINARY)" )
                    paramTreeNode:add( f_paramType, paramType )
                    paramTreeNode:add( f_varlen, param[0] )
                    paramTreeNode:add( f_varbinary, param[1] )
                elseif i_paramType == -99 then -- array
                    -- tree already been built.
                elseif i_paramType == 1 then -- null
                    local paramTreeNode = paramSubtree:add( f_param ):set_text( "Parameter " .. i .. " (NULL)" )
                else
                    local paramTreeNode = paramSubtree:add( f_param, paramsBuf( offset-paramLength, paramLength ) ):set_text( "Parameter " .. i .. " (" .. VOLT_TYPE[i_paramType] .. ")" )
                    paramTreeNode:add( f_paramType, paramType )
                    if i_paramType == 22 then -- decimal
                        paramTreeNode:add( f_decimal, param ) : set_text("Decimal Value: " .. getDecimal(param) )
                    else
                        local typeDescriptor = VOLT_TYPE_DESC[ i_paramType ]
                        paramTreeNode:add( typeDescriptor, param )
                    end
                end
            end
        end -- end for
        return true
    end

    -- Dissector for invocation requests
    local function decodeInvocationRequest( msgBody, pkt, msgSubtree )
        local msgBodyLen = msgBody:len()
        local offset = 0

        -- Get the procedure name (string)
        local v_procNameLen, v_procName
        offset, v_procNameLen, v_procName = nextLengthPrecededValue( msgBody, offset )
        if v_procName == nil then return false end
        local strProcName = v_procName:string()

        -- Get the client data (8 bytes)
        local v_clientData
        offset, v_clientData = nextValue( msgBody, offset, 8 )
        if v_clientData == nil then return false end

        -- Get the parameter count (2 bytes)
        local v_paramsCount
        local parameterCountSize = 2 -- 2 bytes.
        offset, v_paramsCount = nextValue( msgBody, offset, parameterCountSize )
        if v_paramsCount == nil then return false end
        local i_paramsCount = v_paramsCount:int()
        if i_paramsCount < 0 then return false end

        local paramsBuf
        if i_paramsCount > 0 then paramsBuf = msgBody( offset, msgBodyLen - offset ) end
        -- Begin to build the invocation request information tree
        pkt.cols.info:set( "VoltDB Invocation Request (" .. strProcName .. ")" )
        msgSubtree:append_text( "Invocation Request (" .. strProcName .. ")" )
        msgSubtree:add( f_varlen, v_procNameLen ):set_text( "Name Length: " .. v_procNameLen:int() )
        msgSubtree:add( f_str, v_procName ):set_text( "Procedure Name: " .. strProcName )
        msgSubtree:add( f_clientData, v_clientData )

        -- build the parameter subtree
        local paramSubtree = msgSubtree:add( f_params, msgBody( offset - parameterCountSize, msgBodyLen - offset + parameterCountSize ) )
        paramSubtree:set_text( "Parameter: " .. i_paramsCount .. " parameter" .. ( i_paramsCount > 1 and "s" or "" ) ) 
        paramSubtree:add( f_paramsCount, v_paramsCount )
        return buildInvocationRequestParamsTree( i_paramsCount, paramsBuf, paramSubtree )
    end

    -- Dissector for a message
    local function decodeMessage( message, pkt, msgSubtree )
        local bufLen = message:len()
        local offset = 0

        -- Get message length
        local msgLen
        offset, msgLen = nextValue( message, offset, 4 )
        local msgLenValue = msgLen:int()
        -- This message length value doesn't include the 4-byte message length field.
        if msgLenValue + 4 ~= bufLen then return false end
        -- The message length doesn't agree with the buffer size.
        msgSubtree:add( f_msgLen, msgLen ):append_text( " bytes" )

        -- Get the protocol version number (1 byte)
        local v_version
        offset, v_version = nextValue( message, offset, 1 )
        if v_version == nil then return false end
        msgSubtree:add( f_version, v_version )

        -- Get more details from msgBody
        local msgBody
        offset, msgBody = nextValue( message, offset, msgLenValue - 1 )
        if msgBody == nil then return false end

        -- Try to decode as invocation request? invocation response? login request? login response?
        -- We have to try like this because there is no flag indicating the type of the packet
        -- in current protocol design.
        if decodeInvocationRequest( msgBody, pkt, msgSubtree ) then return true end
        if decodeInvocationResponse( msgBody, pkt, msgSubtree ) then return true end
        if decodeLoginRequest( msgBody, pkt, msgSubtree ) then return true end
        if decodeLoginResponse( msgBody, pkt, msgSubtree ) then return true end
        return false
    end

    local function decodePacket( buf, pkt, voltTree )
        local bufLen = buf:len()
        local offset = 0

        while true do
            if not checkBuffer(bufLen, offset, 4) then return end
            -- There is not enough space for another 4-byte message lenght field, end of the packet dissection.
            local msgLen, message
            -- Get next 4-byte message length (there might be multiple messages in one packet)
            msgLen = buf( offset, 4 )
            local msgLenValue = msgLen:int()
            -- This message length value doesn't include the 4-byte message length field.
            msgCount = msgCount + 1

            local msgSubtree
            if msgLenValue <= 0 then
            -- Invalid message length.
                msgSubtree = voltTree:add( f_msg, msgLen )
                msgSubtree:set_text( "Message " .. msgCount .. ": Negative or zero message length (" .. msgLenValue .. " bytes)" )
                hasException = true
                return
            else -- msgLenValue > 0
                if not checkBuffer(bufLen, offset, msgLenValue + 4) then -- Insufficient buffer space
                    msgSubtree = voltTree:add( f_msg, msgLen )
                    msgSubtree:set_text( "Message " .. msgCount .. ": Message length exceeded buffer length (" .. msgLenValue .. " bytes)" )
                    hasException = true
                    return
                else
                    offset, message = nextValue( buf, offset, msgLenValue + 4 )
                    msgSubtree = voltTree:add( f_msg, message ):set_text("Message " .. msgCount .. " (" .. msgLenValue + 4 .. " bytes): " )
                    if not decodeMessage( message, pkt, msgSubtree ) then
                        hasException = true
                        msgSubtree:append_text( "Message parsing encountered exceptions." )
                    end
                end
            end -- end if msgLenValue <= 0
        end -- end while
    end

    local function finalize( pkt )
        if msgCount > 1 then
            pkt.cols.info:set( msgCount .. " VoltDB client-server messages" )
        end
        if hasException then
            pkt.cols.info:append( " with exceptions" )
        end
    end

    -- The main dissector entrance
    function p_voltcw.dissector( buf, pkt, root ) 
        local bufLen = buf:len()
        msgCount = 0
        -- Number of messages in the packet.
        hasException = false
        -- Keep record if any exception happened during the parsing.

        -- Initialize the column display texts and the detail information tree.
        pkt.cols.protocol = "VoltDB"
        pkt.cols.info:set( "VoltDB client-server message" )
        local voltTree = root:add( p_voltcw, buf() )
        voltTree:append_text(" (" .. bufLen .. "-byte packet)")

        -- Begin to decode the packet and put all the details under voltTree.
        decodePacket( buf, pkt, voltTree )
        -- Update the text information according to the values of msgCount and hasException.
        finalize( pkt )
    end
    
    -- The dissector deal with packets from or to port 21212.
    -- We can add a preference setting here to change the port number.
    local tcp_encap_table = DissectorTable.get( "tcp.port" )
    tcp_encap_table:add( 21212, p_voltcw )
end
