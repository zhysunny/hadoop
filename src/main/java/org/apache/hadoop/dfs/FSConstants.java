/**
 * Copyright 2005 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.dfs;

import org.apache.hadoop.util.Constants;

/**
 * 一些方便的常量
 * @author 章云
 * @date 2019/8/9 15:06
 */
interface FSConstants {
    int BLOCK_SIZE = 32 * 1000 * 1000;
    int MIN_BLOCKS_FOR_WRITE = 5;

    long WRITE_COMPLETE = 0xcafae11a;

    //
    // IPC Opcodes 
    //
    // Processed at namenode
    byte OP_ERROR = (byte) 0;
    byte OP_HEARTBEAT = (byte) 1;
    byte OP_BLOCKRECEIVED = (byte) 2;
    byte OP_BLOCKREPORT = (byte) 3;
    byte OP_TRANSFERDATA = (byte) 4;

    // Processed at namenode, from client
    byte OP_CLIENT_OPEN = (byte) 20;
    byte OP_CLIENT_STARTFILE = (byte) 21;
    byte OP_CLIENT_ADDBLOCK = (byte) 22;
    byte OP_CLIENT_RENAMETO = (byte) 23;
    byte OP_CLIENT_DELETE = (byte) 24;
    byte OP_CLIENT_COMPLETEFILE = (byte) 25;
    byte OP_CLIENT_LISTING = (byte) 26;
    byte OP_CLIENT_OBTAINLOCK = (byte) 27;
    byte OP_CLIENT_RELEASELOCK = (byte) 28;
    byte OP_CLIENT_EXISTS = (byte) 29;
    byte OP_CLIENT_ISDIR = (byte) 30;
    byte OP_CLIENT_MKDIRS = (byte) 31;
    byte OP_CLIENT_RENEW_LEASE = (byte) 32;
    byte OP_CLIENT_ABANDONBLOCK = (byte) 33;
    byte OP_CLIENT_RAWSTATS = (byte) 34;
    byte OP_CLIENT_DATANODEREPORT = (byte) 35;
    byte OP_CLIENT_DATANODE_HINTS = (byte) 36;

    // Processed at datanode, back from namenode
    byte OP_ACK = (byte) 40;
    byte OP_TRANSFERBLOCKS = (byte) 41;
    byte OP_INVALIDATE_BLOCKS = (byte) 42;
    byte OP_FAILURE = (byte) 43;

    // Processed at client, back from namenode
    byte OP_CLIENT_OPEN_ACK = (byte) 60;
    byte OP_CLIENT_STARTFILE_ACK = (byte) 61;
    byte OP_CLIENT_ADDBLOCK_ACK = (byte) 62;
    byte OP_CLIENT_RENAMETO_ACK = (byte) 63;
    byte OP_CLIENT_DELETE_ACK = (byte) 64;
    byte OP_CLIENT_COMPLETEFILE_ACK = (byte) 65;
    byte OP_CLIENT_TRYAGAIN = (byte) 66;
    byte OP_CLIENT_LISTING_ACK = (byte) 67;
    byte OP_CLIENT_OBTAINLOCK_ACK = (byte) 68;
    byte OP_CLIENT_RELEASELOCK_ACK = (byte) 69;
    byte OP_CLIENT_EXISTS_ACK = (byte) 70;
    byte OP_CLIENT_ISDIR_ACK = (byte) 71;
    byte OP_CLIENT_MKDIRS_ACK = (byte) 72;
    byte OP_CLIENT_RENEW_LEASE_ACK = (byte) 73;
    byte OP_CLIENT_ABANDONBLOCK_ACK = (byte) 74;
    byte OP_CLIENT_RAWSTATS_ACK = (byte) 75;
    byte OP_CLIENT_DATANODEREPORT_ACK = (byte) 76;
    byte OP_CLIENT_DATANODE_HINTS_ACK = (byte) 77;

    // Processed at datanode stream-handler
    byte OP_WRITE_BLOCK = (byte) 80;
    byte OP_READ_BLOCK = (byte) 81;
    byte OP_READSKIP_BLOCK = (byte) 82;

    // Encoding types
    byte RUNLENGTH_ENCODING = 0;
    byte CHUNKED_ENCODING = 1;

    // Return codes for file create
    int OPERATION_FAILED = 0;
    int STILL_WAITING = 1;
    int COMPLETE_SUCCESS = 2;

    //
    // Timeouts, constants
    //
    long HEARTBEAT_INTERVAL = 3 * 1000;
    long EXPIRE_INTERVAL = 10 * 60 * 1000;
    long BLOCKREPORT_INTERVAL = 60 * 60 * 1000;
    long DATANODE_STARTUP_PERIOD = 2 * 60 * 1000;
    long LEASE_PERIOD = 60 * 1000;
    int READ_TIMEOUT = 60 * 1000;

    int BUFFER_SIZE = Constants.IO_FILE_BUFFER_SIZE;

}

