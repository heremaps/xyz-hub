/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.models.geojson;


import com.here.xyz.models.geojson.coordinates.BBox;

public class HQuad {

    /**
     * HereQuad maximum zoom level. Level is limited by {@code long} key representation. Equals to
     * ~0.0187 m per quad at the equator.
     */
    private static final int MAX_LEVEL = 31;

    /** The pixel X-coordinate in the level of detail. */
    public int x;
    /** The pixel y-coordinate in the level of detail. */
    public int y;
    /** The level of detail. */
    public int level;

    public String quadkey;
    private long hkey;

    public HQuad(String quadkey, boolean isBase4Encoded) {
        if (!isBase4Encoded) {
            // Decode to Base4
            quadkey = Long.toString(Long.parseLong(quadkey, 10), 4);
            // Cut Leading 1
            quadkey = quadkey.substring(1);
        }

        if (quadkey == null || !quadkey.matches("[0123]{1,31}"))
            throw new IllegalArgumentException("Quadkey '" + quadkey + "' is invalid!");

        this.level = quadkey.length();
        this.hkey = MortonUtlis.convertQuadKeyToLongKey(quadkey) | (1L << (level * 2));
        this.quadkey = quadkey;

        /** Remove first level bit and convert to x */
        this.x = MortonUtlis.getX(hkey & ((1L << level * 2) - 1));
        /** Remove first level bit and convert to y */
        this.y = MortonUtlis.getY(hkey & ((1L << level * 2) - 1));
        validate();
    }

    public HQuad(int x, int y, int level) {
        this.x = x;
        this.y = y;
        this.level = level;

        /** Add LevelBit and convert it to long key */
        this.hkey = MortonUtlis.convertXYToLongKey(x, y) | (1L << (level * 2));
        this.quadkey = MortonUtlis.convertLongKeyToQuadKey(hkey, level);
        validate();
    }

    private void validate() {
        if (level < 0 || level > 31)
            throw new IllegalArgumentException(
                    "Level not valid! " + level + " is outside from bounds: [0," + MAX_LEVEL + "]");

        long xMax = ((1L << level) - 1);
        long yMax = (((1L << level) - 1) / 2);

        if (x < 0 || x > xMax)
            throw new IllegalArgumentException("X not valid! " + x + " is outside from bounds: [0," + xMax + "]");

        if (y < 0 || y > yMax)
            throw new IllegalArgumentException("Y not valid! " + y + " is outside from bounds [0," + yMax + "]");

        if (quadkey == null || !quadkey.matches("[0123]{1,31}"))
            throw new IllegalArgumentException("Quadkey '" + quadkey + "' is invalid!");
    }

    public BBox getBoundingBox() {
        double width = 360.0 / (1L << level);
        double heigth = level == 0 ? 180 : 360.0 / (1L << level);

        double west = width * x - 180;
        double south = heigth * y - 90;
        double east = width * (x + 1) - 180;
        double north = heigth * (y + 1) - 90;

        return new BBox(west, south, east, north);
    }

    @Override
    public String toString() {
        return "HereQuad [quadkey="
                + quadkey
                + ", longKey="
                + hkey
                + ", x="
                + x
                + ", y="
                + y
                + ", zoomLevel="
                + level
                + "]";
    }

    /**
     * Eg.: X = 754 = 1011110010 Y = 531 = 1000010011
     *
     * <p>Morton code: 1100 0101 0111 0000 1110 (interleave X an Y bits starting from right). Quadkey:
     * 3 0 1 1 1 3 0 0 3 2 (combine each bit-pair)
     *
     * <p>Zoom-Level Indicator: Morton code: 1100 0101 0111 0000 1110 Add leading bit 11100 0101 0111
     * 0000 1110 => first bit (2^21) gets used to calculate tile zoom level
     */
    public static class MortonUtlis {
        /** Magic numbers to interleave bits of 64 bit numbers. */
        private static long magicNumbers[] = {
            0x5555555555555555L,
            0x3333333333333333L,
            0x0F0F0F0F0F0F0F0FL,
            0x00FF00FF00FF00FFL,
            0x0000FFFF0000FFFFL,
            0x00000000FFFFFFFFL
        };

        public static long convertQuadKeyToLongKey(String quadKey) {
            long longKey = 0;
            for (int i = 0, k = quadKey.length() - 1; i < quadKey.length(); i++, k--) {
                long currentLevelValue = Long.parseLong(quadKey.substring(k, k + 1));
                longKey += currentLevelValue << i * 2;
            }
            return longKey;
        }

        public static String convertLongKeyToQuadKey(long longKey, int zoomLevel) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= zoomLevel; i++) {
                builder.insert(0, longKey & 0b11);
                longKey = longKey >> 2;
            }
            return builder.toString();
        }

        public static long convertXYToLongKey(int x, int y) {
            long X = interleaveToEvenBits(x);
            long Y = interleaveToEvenBits(y);
            return X | (Y << 1);
        }

        public static int getX(long longKey) {
            return (int) extractEvenBits(longKey);
        }

        public static int getY(long longKey) {
            return (int) extractEvenBits(longKey >> 1);
        }

        private static long extractEvenBits(long number) {
            number = number & magicNumbers[0];
            number = (number | (number >> 1)) & magicNumbers[1];
            number = (number | (number >> 2)) & magicNumbers[2];
            number = (number | (number >> 4)) & magicNumbers[3];
            number = (number | (number >> 8)) & magicNumbers[4];
            number = (number | (number >> 16)) & magicNumbers[5];
            return number;
        }

        private static long interleaveToEvenBits(long number) {
            number = (number | (number << 16)) & magicNumbers[4];
            number = (number | (number << 8)) & magicNumbers[3];
            number = (number | (number << 4)) & magicNumbers[2];
            number = (number | (number << 2)) & magicNumbers[1];
            number = (number | (number << 1)) & magicNumbers[0];
            return number;
        }
    }
}
