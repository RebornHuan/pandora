/*
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
package my.skypiea.punygod.yarn.deploy.util;

public class Constants {

  public static final String DEFAULT_APP_NAME = "Datax";
  public static final String DEFAULT_APP_MASTER_MEMORY = "2048";
  public static final String DEFAULT_APP_MASTER_VCORES = "1";
  public static final String DEFAULT_APP_MASTER_QUEUE = "default";
  public static final String DEFAULT_CONTAINER_MEMORY = "2048";
  public static final String DEFAULT_CONTAINER_VCORES = "1";
  public static final String DEFAULT_DATAX_WORKER_NUM = "2";
  public static final String DEFAULT_DATAX_PS_NUM = "1";

  public static final String OPT_DATAX_APP_NAME = "app_name";
  public static final String OPT_DATAX_APP_MASTER_MEMORY = "am_memory";
  public static final String OPT_DATAX_APP_MASTER_VCORES = "am_vcores";
  public static final String OPT_DATAX_APP_MASTER_QUEUE = "am_queue";
  public static final String OPT_DATAX_CONTAINER_MEMORY = "container_memory";
  public static final String OPT_DATAX_CONTAINER_VCORES = "container_vcores";
  public static final String OPT_DATAX_JAR = "datax_jar";
  public static final String OPT_DATAX_WORKER_NUM = "num_worker";
  public static final String OPT_DATAX_PS_NUM = "num_ps";
  public static final String OPT_APPLICATION_ID = "app_id";

  public static final String OPT_JOB_NAME = "job_name";
  public static final String DATAX_JAR_NAME = "datax.jar";
}
