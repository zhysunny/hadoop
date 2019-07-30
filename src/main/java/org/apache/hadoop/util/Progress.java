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

package org.apache.hadoop.util;

import java.util.ArrayList;

/**
 * 帮助生成进度报告的实用程序。
 * 应用程序构建一个由{@link Progress}实例组成的层次结构，每个实例建模一个执行阶段。
 * root是用{@link #Progress()}构造的。
 * 子阶段的节点是通过调用{@link #addPhase()}创建的。
 * @author 章云
 * @date 2019/7/30 22:29
 */
public class Progress {
    private String status = "";
    private float progress;
    private int currentPhase;
    private ArrayList<Progress> phases = new ArrayList<Progress>();
    private Progress parent;
    private float progressPerPhase;

    public Progress() {
    }

    /**
     * 将命名节点添加到树中。
     * @param status
     * @return
     */
    public Progress addPhase(String status) {
        Progress phase = addPhase();
        phase.setStatus(status);
        return phase;
    }

    /**
     * 向树中添加节点。
     * @return
     */
    public Progress addPhase() {
        Progress phase = new Progress();
        phases.add(phase);
        phase.parent = this;
        progressPerPhase = 1.0f / (float) phases.size();
        return phase;
    }

    /**
     * 在执行期间调用，以移动到树中此级别的下一个阶段。
     */
    public void startNextPhase() {
        currentPhase++;
    }

    /**
     * 返回当前正在执行的子节点。
     * @return
     */
    public Progress phase() {
        return phases.get(currentPhase);
    }

    /**
     * 完成此节点，将父节点移动到它的下一个子节点。
     */
    public void complete() {
        progress = 1.0f;
        if (parent != null) {
            parent.startNextPhase();
        }
    }

    /**
     * 在叶子节点上执行时调用，以设置其进度。
     * @param progress
     */
    public void set(float progress) {
        this.progress = progress;
    }

    /**
     * 返回root的总体进度。
     * @return
     */
    public float get() {
        Progress node = this;
        while (node.parent != null) {                 // find the root
            node = parent;
        }
        return node.getInternal();
    }

    /**
     * 计算此节点中的进度。
     * @return
     */
    private float getInternal() {
        int phaseCount = phases.size();
        if (phaseCount != 0) {
            float subProgress = currentPhase < phaseCount ? phase().getInternal() : 0.0f;
            return progressPerPhase * (currentPhase + subProgress);
        } else {
            return progress;
        }
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        toString(result);
        return result.toString();
    }

    private void toString(StringBuffer buffer) {
        buffer.append(status);
        if (phases.size() != 0 && currentPhase < phases.size()) {
            buffer.append(" > ");
            phase().toString(buffer);
        }
    }

}
