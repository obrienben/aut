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

import com.google.common.io.Resources
import org.apache.spark.sql.SparkSession
// scalastyle:off underscore.import
import io.archivesunleashed.df._
import org.apache.spark.sql.functions._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractVideoDetailsTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.media.warc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-df"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  test("Video DF extraction") {
    val df = RecordLoader.loadArchives(warcPath, sc)
      .extractVideoDetailsDF()

    val extracted = df.select("url", "filename", "extension", "mime_type", "md5")
      .orderBy(desc("md5")).head(1).toList
    assert(extracted.size == 1)
    assert("https://ruebot.net/2018-11-12%2016.14.11.mp4" == extracted(0)(0))
    assert("2018-11-12%2016.14.11.mp4" == extracted(0)(1))
    assert("mp4" == extracted(0)(2))
    assert("video/mp4" == extracted(0)(3))
    assert("2cde7de3213a87269957033f6315fce2" == extracted(0)(4))
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
