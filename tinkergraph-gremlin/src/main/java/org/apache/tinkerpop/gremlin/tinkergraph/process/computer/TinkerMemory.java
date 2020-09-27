/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerMemory implements Memory.Admin {

    //保存所有有效的key，perviousMap和currentMap中的key需要在此范围中
    public final Set<String> memoryKeys = new HashSet<>();
    //保存原始数据状态，当某一个阶段完成则更新previousMap
    public Map<String, Object> previousMap;
    //保存当前计算中的数据
    public Map<String, Object> currentMap;
    //迭代计数
    private final AtomicInteger iteration = new AtomicInteger(0);
    //任务执行时间
    private final AtomicLong runtime = new AtomicLong(0l);

    public TinkerMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers) {
        this.currentMap = new ConcurrentHashMap<>();
        this.previousMap = new ConcurrentHashMap<>();
        //??
        if (null != vertexProgram) {
            //MemoryComputeKeys存在于整个计算声明周期
            for (final String key : vertexProgram.getMemoryComputeKeys()) {
                MemoryHelper.validateKey(key);
                this.memoryKeys.add(key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            //MemoryKey 为mapReducer任务输出结果对应的key，对应一个mapReduce任务
            this.memoryKeys.add(mapReduce.getMemoryKey());
        }
    }

    @Override
    public Set<String> keys() {
        return this.previousMap.keySet();
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
        this.previousMap = this.currentMap;
    }

    protected void completeSubRound() {
        this.previousMap = new ConcurrentHashMap<>(this.currentMap);

    }

    @Override
    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.previousMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    /**
     * 更新currentMap中的value
     * 如果key不存在于memoryKeys中，则异常，否则进行更新
     * 更新存在三种类型（long，bool，object）
     * @param key   the key of the long value
     * @param delta the adjusting amount (can be negative for decrement)
     */
    @Override
    public void incr(final String key, final long delta) {
        checkKeyValue(key, delta);
        this.currentMap.compute(key, (k, v) -> null == v ? delta : delta + (Long) v);
    }

    @Override
    public void and(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        this.currentMap.compute(key, (k, v) -> null == v ? bool : bool && (Boolean) v);
    }

    @Override
    public void or(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        this.currentMap.compute(key, (k, v) -> null == v ? bool : bool || (Boolean) v);
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        this.currentMap.put(key, value);
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }
}
