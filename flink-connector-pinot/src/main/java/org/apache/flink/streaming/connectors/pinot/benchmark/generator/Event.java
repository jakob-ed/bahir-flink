/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pinot.benchmark.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.connectors.pinot.benchmark.BenchmarkEvent;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class Event implements Serializable {

    public Event(Double partition) {
        if (partition > 0) {
            geoList = Arrays.copyOfRange(geoListAll, 0, (int) (geoListAll.length * partition));
        } else {
            geoList = Arrays.copyOfRange(geoListAll, (int) (geoListAll.length * (1 + partition)), geoListAll.length);
        }

    }

    private int geoIndex = 0;
    private Random rand = new Random(93285L);
    //private Random rand = new Random();
    private String[] geoListAll = {
            "AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AC", "AU", "AT", "AZ", "BS", "BH", "BB",
            "BD", "BY", "BE", "BZ", "BJ", "BM", "BT", "BW", "BO", "BA", "BV", "BR", "IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA",
            "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CY", "CZ", "CS",
            "DK", "DJ", "DM", "DO", "TP", "EC", "EG", "SV", "GQ", "ER", "EE", "ET", "EU", "FK", "FO", "FJ", "FI", "FR", "FX", "GF",
            "PF", "TF", "MK", "GA", "GM", "GE", "DE", "GH", "GI", "GB", "GR", "GL", "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY"};
    private String[] geoList = null;

    public String generateJson() throws JsonProcessingException {

        //geo
//        String geo = null;
//        geoIndex = geoIndex % geoList.length;
//        geo = geoList[geoIndex];
//        geoIndex++;

        byte[] array = new byte[155]; // 0.15 -> 57, 0.25 -> 155
        new Random().nextBytes(array);
        String geo = new String(array, StandardCharsets.UTF_8);

        //price
        float minX = 5.0f;
        float maxX = 100.0f;
        float finalX = rand.nextFloat() * (maxX - minX) + minX;
        String price = Float.toString(finalX);

        BenchmarkEvent e = new BenchmarkEvent(price, geo, System.currentTimeMillis(), Long.MAX_VALUE);
        return JsonUtils.objectToString(e);
    }
}
