/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.archivesunleashed

import java.io.BufferedWriter
import java.io.FileWriter
import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class RecordLoaderFromListTest extends FunSuite with BeforeAndAfter {
//  private val listFile = "warc/warcs-list.txt"
  private val listFile = Resources.getResource("warc/warcs-list.txt").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)


    val warcResources = List[String]("warc/example.warc.gz", "warc/example.docs.warc.gz", "warc/example.media.warc.gz",
      "warc/example.pdf.warc.gz", "warc/example.txt.warc.gz")
    val warcPaths = for (w <- warcResources) yield Resources.getResource(w).getPath.concat("\n")

    val writer = new BufferedWriter(new FileWriter(listFile))
    warcPaths.foreach(writer.write)
    writer.close()

  }

  test("loads Warcs from file") {
    val base = RecordLoader.loadArchivesFromList(listFile, sc)
      .keepValidPages()
      .map(x => x.getUrl)
      .take(1)
    assert(base(0) == "http://www.archive.org/")
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
