/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    // 通过该index文件的结构. 可以了解到,索引文件本质上是一个hashmap.的文件存储实现模式。
    //    从索引槽到行内容,索引槽 对应的是 hashmap的 table索引槽,
    //    索引行中存储了pre_content_index的指针, 用于解决hash冲突,
    //    table索引槽中存储 最后一个hash的 content_index指针.
    //
    // 对索引文件中的key进行设置. phyOffset为 commitLog中的地址信息. storeTime为commitLog 入库时间
    // 此处的重点为. 后续通过key进行计算过滤怎么处理。
    // 换句话说. 索引的数据结构是怎么样的。
    // 从 下方计算可以推测出. rocketmq 对于index文件的规则可以参考下方
    // |--------------header---index_header(40字节)------------------|
    // |--------------header---hash_slot----------------------------|
    // |--------------content_0-------------------------------------|
    // |--------------content_1-------------------------------------|
    // |--------------content_2-------------------------------------|
    // |--------------content_3-------------------------------------|
    // 其中头部 部分可以分为两种.
    //    第一种为 索引文件描述,内容固定,和索引内容基本无关. 总大小为40字节
    //    第二种为 索引槽 数据总大小为 单个槽大小*最大槽总数. 其中单个槽大小固定为4字节,槽总数通过messageStoreConfig 传入。
    //      索引槽 内容为查询key的 hash值. 所以单个槽内容为4字节.
    //      既然 索引槽内容为hash值, 那么可以大胆的推断出 index文件使用 hash计算得到该key的相对于槽而言的位置大小,通过槽内的内容也就可以知道.该key所对应数据的真实文件位置,解释按照下方给出
    // 其中除却头部部分,其他均为索引内容(索引行内容方案 ☆☆☆☆☆)
    //    平时通过内存中的hash,list结构很容易找出值在哪里.如果需要放在文件系统中呢？并且如果内容长度是可变的话,怎么找寻呢？
    //    如果需要考虑可变内容,那么还需要在额外存放行内容大小,如果是按照list顺序遍历的方式,这样是没问题的. 但是这样就导致每次查询都从0还是,查询效率就非常的慢了,
    //       当然也可以做二分查找,但是总共需要跳过的字节数还是需要计算出来
    ////       |content0_index|content0_size|content0_data|  40 字节 0~40
    ///        |content1_index|content1_size|content1_data|  60 字节 40~100
    ///        |content2_index|content2_size|content2_data|  80 字节 100~180
    ///        查找一个数据需要通过 0开始遍历对size进行累加才能计算出 需要跳过的数据  比如需要查找content_2的数据,需要对content_0和content_1的数据进行累加 =100,这个数据为跳过数据
    //    那么就可以考虑规避这个问题,使用固定长度的`内容行` 此时就有计算规则了. 文件行的内容起始位置=文件头部(包含索引槽和索引文件描述)+偏移地址*内容长度
    //         |content0_index|content0_data|  40 字节 0~40
    //         |content1_index|content1_data|  40 字节 40~80
    //         |content2_index|content2_data|  40 字节 80~120
    //         |content3_index|content3_data|  40 字节 120~160
    //       通过固定长度的内容行. 可以很便捷计算出需要跳过的数据  同样按照查找 content_2为例. 需要跳过的数据 2 * 40 = 80 并不需要针对每个进行累加
    // 上述方案概要完成, 可以细则解释
    // Q1 文件头部存放了什么?
    // A1 index_header 主要存放是的整体索引文件 元数据信息。
    //   其中包含
    //   1. beginTimestampIndex 第一个写入消息时创建索引文件创建时间戳. 使用的是 long 占用 8字节
    //   2. endTimestampIndex 索引文件最后写入的时间戳, 使用的是 long 占用8字节
    //   3. beginPhyoffsetIndex 第一个写入消息的物理地址  使用的是 long 占用8字节
    //   4. endPhyoffsetIndex 最后一个写入消息的物理地址  使用的是 long 占用8字节
    //   5. hashSlotcountIndex 索引槽的总数 已经使用的索引槽. 配置文件内有固定的索引槽. 但是对于文件内真实存在的就需要文件自己记录,用于后续使用 使用的是int 占用4字节
    //   6. indexCountIndex 索引内容的总数 已经使用的索引内容. 该数据一般和`hashSlotcountIndex` 是一样的。   使用的是int 占用4字节
    // Q2 索引槽中到底存放了什么?
    // A2 索引槽存放是的 内容行的编号. 每次新增一个index的时候 index_header中的index_count就需要增加一个.然后存放在对应的文件槽内。
    // Q3 content 固定长度后包含哪些东西?一个索引文件中包含哪些数据
    // A3 content 内容包含.
    // 1. ketHash.该内容行所使用的hash值. ====>类似于使用了分表也要记录根据什么条件进行的分表
    // 2. phyOffset 内容行commitLog 物理地址 ====>为了寻找到具体的消息
    // 3. timeDiff 内容行commitLog 落库时间相对于索引文件的间隔时间====> 用于过期？间隔时间短的表示最初进入的. 当出现hash冲突的时候提供一个去处逻辑？
    // 4. slotValue 存储的上一次hash计算出的 文件行具体地址。 ☆☆☆☆☆ 重要.
    // Q4 从上描述得到的信息可以该方案有一个严重的弊端(仅仅使用content_index去定位数据). 当出现hash冲突后怎么去解决？？？
    // A4 在hash冲突的行内. 记录上一次文件行的content_index,这样在一个hash槽内就组成了一个链表结构。并且这部分是属于头插法====>
    //       将最新的记录插入到hash槽内, 再辅佐于keyHash真实的值. 就可以定位到数据了
    //  举例. 现在有四个key. 这四个key对应的{k0,k1,k2,k3} hash都落入到了一个hash_slot[1]中
    //  那么hash_slot[1] 的变化在进行四次插入后有如下变化：
    //
    //  hash_slot[1]=content0_index => hash_slot[1]=content1_index => hash_slot[1]=content2_index => hash_slot[1]=content2_index
    //         |-------------[hash_slot[1]]--------------------------------------|
    //         | 索引行,行标index_count|内容   |hash值|上一个索引行,航标   |
    //         |content0_index|content0_data|keyHash_0|-1            |
    //         |content1_index|content1_data|keyHash_1|content0_index|
    //         |content2_index|content2_data|keyHash_2|content1_index|
    //         |content3_index|content3_data|keyHash_3|content2_index|
    //
    // 逻辑上 索引文件的 索引槽头部和索引内容是一个 hashmap. 采用的是头插法
    //
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 计算这个key的字符串hash
            int keyHash = indexKeyHashMethod(key);
            // 计算出hash所在的桶是多少
            int slotPos = keyHash % this.hashSlotNum;
            //   抽象的概念的 桶坐标？.绝对地址？？？不是很明白这个.====>hashSlotNum 这个是配置的. 通过indexService传入,indexService中的由MessageStoreConfig传入。
            // 通过简单计算理解 absSlotPos表示的是 在Index这个文件中. 的物理地址。其中slotPos是逻辑地址,表示的是桶的序号.然后因为每个slot是固定长度的.
            //   那么就可以通过slotPos*slotSize的方式得到这个数据的绝对地址 再然后因为每个index文件有头部信息, 这个需要跳过的,
            // 一般这么操作,是因为数据最终是需要落地在文件中的. 采用一行一个数据的方式是可以的,但是这样的数据是非紧凑型的.
            //   那么就引发了一个问题. 为什么index需要使用这样的方式进行 读写？对于index这样高频读写的数据, 不太适合
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {
                // 从被注释掉的痕迹看.  原本是打算 文件锁的.☆☆☆☆ 文件锁是系统编程基础,需要着重看
                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 存在hash冲突如何处理
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
