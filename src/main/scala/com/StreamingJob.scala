/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com

import org.apache.flink.streaming.api.scala._

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // 第一步：设定执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //第二步：指定数据源，读取数据
    val source = env.readTextFile("E:\\Idea\\FlinkWordCount\\word.txt")

    //第三步：对数据进行处理
    source
      .flatMap(line => line.toLowerCase.split("\\W+"))
      .filter(_.length > 0)
      .map(word => (word, 1))
      .keyBy(0)
      .sum(1)
      //第四部：输出结果
//      .writeAsText("E:\\Idea\\FlinkWordCount\\word.txt\\word_out.txt")
      .print()


    // 第五步：指定任务名称并触发流式任务
    env.execute("Streaming Scala WordCount")
  }
}
