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

package com.here.xyz.psql.factory;

public class TweaksSQL
{
  public static final String SAMPLING = "sampling";
  public static final String SAMPLING_STRENGTH = "strength";
  public static final String SAMPLING_ALGORITHM = "algorithm";
  public static final String SAMPLING_ALGORITHM_DST = "distribution";
  public static final String SAMPLING_ALGORITHM_SZE = "geometrysize";
  public static final String SIMPLIFICATION = "simplification";
  public static final String SIMPLIFICATION_STRENGTH = SAMPLING_STRENGTH;
  public static final String SIMPLIFICATION_ALGORITHM = "algorithm";
  public static final String SIMPLIFICATION_ALGORITHM_A01 = "grid";
  public static final String SIMPLIFICATION_ALGORITHM_A02 = "simplifiedkeeptopology";
  public static final String SIMPLIFICATION_ALGORITHM_A03 = "simplified";
  

  /*
   [  1     |   (1) | 1/3    | ~ md5( '' || i) < '5'   ]
   [  5     |   (5) | 1/4    | ~ md5( '' || i) < '4'   ]
   [ low    |  (10) | 1/8    | ~ md5( '' || i) < '2'   ]
   [ lowmed	|  (30) | 1/32   | ~ md5( '' || i) < '08'  ]
   [ med    |  (50) | 1/128  | ~ md5( '' || i) < '02'  ]
   [ medhigh|  (75) | 1/1024 | ~ md5( '' || i) < '004' ]
   [ high   | (100) | 1/4096 | ~ md5( '' || i) < '001' ]
  */

  private static final String DstFunctIndexExpr = "left(md5(''||i),5)"; 
  public static String strengthSql(int strength, boolean bRandom)
  { 
   if( !bRandom ) 
   {
    double bxLen = ( strength <=  5  ? 0.001  : 
                     strength <= 10  ? 0.002  : 
                     strength <= 30  ? 0.004  : 
                     strength <= 50  ? 0.008  :
                     strength <= 75  ? 0.01   : 0.05 );
    return String.format("( ST_Perimeter(box2d(geo) ) > %f )", bxLen );              
   }
    
   String s = ( strength <=  1  ? "5"   : 
                strength <=  5  ? "4"   : 
                strength <= 10  ? "2"   : 
                strength <= 30  ? "08"  :
                strength <= 50  ? "02"  :
                strength <= 75  ? "004" : "001" );
     
   return String.format("%s < '%s'",DstFunctIndexExpr,s);
  }
}



