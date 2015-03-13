/*
 * BRCache http://brcache.brandao.org/
 * Copyright (C) 2015 Afonso Brandao. (afonso.rbn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brandao.brcache.collections;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author Brandao
 */
public class Collections {

    public static final String DEFAULT_TMP_DIR = System.getProperty("java.io.tmpdir");
    private static boolean initialized = false;
    private static Map<String, Object> locks = new LinkedHashMap<String, Object>();
    private static File path;
    private static String defaultPath = DEFAULT_TMP_DIR + "/collections";
    private static File defaultFilePath = new File(defaultPath);
    private static int defaultSegmentSize = 120;
    private static long collectionID = 0;

    public static String getNextId() {

        if (!initialized) {
            initialize();
        }

        return String.valueOf(System.currentTimeMillis()) + String.valueOf(collectionID++);
    }

    public static void setPath(String aPath) {
        path = new File(aPath);
        if (!path.exists()) {
            path.mkdirs();
        }
    }

    public static File getPath() {
        return path == null ? defaultFilePath : path;
    }

    public static void registerLock(String id, Object lock) {
        locks.put(id, lock);
    }

    public static Object getLock(String id) {
        return locks.remove(id);
    }

    public static int getDefaultSegmentSize() {
        return defaultSegmentSize;
    }

    public static void setDefaultSegmentSize(int aDefaultSegmentSize) {
        defaultSegmentSize = aDefaultSegmentSize;
    }

    private static synchronized void initialize() {
        if (initialized) {
            return;
        }

        initialized = true;
        deleteDir(defaultFilePath);
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
}
