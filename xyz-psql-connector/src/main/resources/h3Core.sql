--
-- Copyright (C) 2017-2020 HERE Europe B.V.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- SPDX-License-Identifier: Apache-2.0
-- License-Filename: LICENSE
--
-- \timing on
-- \set ON_ERROR_STOP on

/*
Sourcecode:  https://github.com/uber/h3.git  tag: v3.2.0
*/

set search_path = h3, public, topology;

create schema if not exists h3;

create or replace function h3_version() 
returns integer as
$body$
 select 106
$body$ 
language sql immutable;

do $body$ 
 begin
  begin

    create domain h3index bigint;

  exception
   when duplicate_object then raise notice 'type h3index already exists';
  end;
  begin

    create type face_ijk_t as ( face integer, i integer, j integer, k integer );

  exception
   when duplicate_object then raise notice 'type face_ijk_t already exists';
  end;
  begin

    create type vec3d_t as (x double precision, y double precision, z double precision );

  exception
   when duplicate_object then raise notice 'type vec3d_t already exists';
  end;
 end;
$body$;


-- NUM_ICOSA_FACES 20

create or replace function _faceCenterGeo(icoface integer, out lat double precision, out lon double precision )
as
$body$
declare
 -- does 15 digits fit???
 faceCenterGeo double precision[][2] = array
 [
    [ 0.803582649718989942,  1.248397419617396099],  -- face  0
    [ 1.307747883455638156,  2.536945009877921159],  -- face  1
    [ 1.054751253523952054, -1.347517358900396623],  -- face  2
    [ 0.600191595538186799, -0.450603909469755746],  -- face  3
    [ 0.491715428198773866,  0.401988202911306943],  -- face  4
    [ 0.172745327415618701,  1.678146885280433686],  -- face  5
    [ 0.605929321571350690,  2.953923329812411617],  -- face  6
    [ 0.427370518328979641, -1.888876200336285401],  -- face  7
    [-0.079066118549212831, -0.733429513380867741],  -- face  8
    [-0.230961644455383637,  0.506495587332349035],  -- face  9
    [ 0.079066118549212831,  2.408163140208925497],  -- face 10
    [ 0.230961644455383637, -2.635097066257444203],  -- face 11
    [-0.172745327415618701, -1.463445768309359553],  -- face 12
    [-0.605929321571350690, -0.187669323777381622],  -- face 13
    [-0.427370518328979641,  1.252716453253507838],  -- face 14
    [-0.600191595538186799,  2.690988744120037492],  -- face 15
    [-0.491715428198773866, -2.739604450678486295],  -- face 16
    [-0.803582649718989942, -1.893195233972397139],  -- face 17
    [-1.307747883455638156, -0.604647643711872080],  -- face 18
    [-1.054751253523952054,  1.794075294689396615]   -- face 19
 ];
begin
 lat = faceCenterGeo[icoface+1][1];
 lon = faceCenterGeo[icoface+1][2];
end;
$body$
language plpgsql immutable;

create or replace function _faceCenterPoint(icoface integer, out x double precision, out y double precision, out z double precision )
as
$body$
declare
 -- does 15 digits fit???
 faceCenterPoint double precision[][3] = array
 [
    [ 0.2199307791404606,  0.6583691780274996,  0.7198475378926182],   -- face  0
    [-0.2139234834501421,  0.1478171829550703,  0.9656017935214205],   -- face  1
    [ 0.1092625278784797, -0.4811951572873210,  0.8697775121287253],   -- face  2
    [ 0.7428567301586791, -0.3593941678278028,  0.5648005936517033],   -- face  3
    [ 0.8112534709140969,  0.3448953237639384,  0.4721387736413930],   -- face  4
    [-0.1055498149613921,  0.9794457296411413,  0.1718874610009365],   -- face  5
    [-0.8075407579970092,  0.1533552485898818,  0.5695261994882688],   -- face  6
    [-0.2846148069787907, -0.8644080972654206,  0.4144792552473539],   -- face  7
    [ 0.7405621473854482, -0.6673299564565524, -0.0789837646326737],   -- face  8
    [ 0.8512303986474293,  0.4722343788582681, -0.2289137388687808],   -- face  9
    [-0.7405621473854481,  0.6673299564565524,  0.0789837646326737],   -- face 10
    [-0.8512303986474292, -0.4722343788582682,  0.2289137388687808],   -- face 11
    [ 0.1055498149613919, -0.9794457296411413, -0.1718874610009365],   -- face 12
    [ 0.8075407579970092, -0.1533552485898819, -0.5695261994882688],   -- face 13
    [ 0.2846148069787908,  0.8644080972654204, -0.4144792552473539],   -- face 14
    [-0.7428567301586791,  0.3593941678278027, -0.5648005936517033],   -- face 15
    [-0.8112534709140971, -0.3448953237639382, -0.4721387736413930],   -- face 16
    [-0.2199307791404607, -0.6583691780274996, -0.7198475378926182],   -- face 17
    [ 0.2139234834501420, -0.1478171829550704, -0.9656017935214205],   -- face 18
    [-0.1092625278784796,  0.4811951572873210, -0.8697775121287253]    -- face 19
 ];
begin
 x = faceCenterPoint[icoface+1][1];
 y = faceCenterPoint[icoface+1][2];
 z = faceCenterPoint[icoface+1][3];
end;
$body$
language plpgsql immutable;

create or replace function _faceAxesAzRadsCII(icoface integer, out v0 double precision, out v1 double precision, out v2 double precision )
as
$body$
declare
 -- does 15 digits fit???
 faceAxesAzRadsCII double precision[][3] = array
 [
    [5.619958268523939882, 3.525563166130744542, 1.431168063737548730],  -- face  0
    [5.760339081714187279, 3.665943979320991689, 1.571548876927796127],  -- face  1
    [0.780213654393430055, 4.969003859179821079, 2.874608756786625655],  -- face  2
    [0.430469363979999913, 4.619259568766391033, 2.524864466373195467],  -- face  3
    [6.130269123335111400, 4.035874020941915804, 1.941478918548720291],  -- face  4
    [2.692877706530642877, 0.598482604137447119, 4.787272808923838195],  -- face  5
    [2.982963003477243874, 0.888567901084048369, 5.077358105870439581],  -- face  6
    [3.532912002790141181, 1.438516900396945656, 5.627307105183336758],  -- face  7
    [3.494305004259568154, 1.399909901866372864, 5.588700106652763840],  -- face  8
    [3.003214169499538391, 0.908819067106342928, 5.097609271892733906],  -- face  9
    [5.930472956509811562, 3.836077854116615875, 1.741682751723420374],  -- face 10
    [0.138378484090254847, 4.327168688876645809, 2.232773586483450311],  -- face 11
    [0.448714947059150361, 4.637505151845541521, 2.543110049452346120],  -- face 12
    [0.158629650112549365, 4.347419854898940135, 2.253024752505744869],  -- face 13
    [5.891865957979238535, 3.797470855586042958, 1.703075753192847583],  -- face 14
    [2.711123289609793325, 0.616728187216597771, 4.805518392002988683],  -- face 15
    [3.294508837434268316, 1.200113735041072948, 5.388903939827463911],  -- face 16
    [3.804819692245439833, 1.710424589852244509, 5.899214794638635174],  -- face 17
    [3.664438879055192436, 1.570043776661997111, 5.758833981448388027],  -- face 18
    [2.361378999196363184, 0.266983896803167583, 4.455774101589558636]   -- face 19
 ];
begin
 v0 = faceAxesAzRadsCII[icoface+1][1];
 v1 = faceAxesAzRadsCII[icoface+1][2];
 v2 = faceAxesAzRadsCII[icoface+1][3];
end;
$body$
language plpgsql immutable;

create or replace function _s_faceIjkBaseCells(icoface integer, i integer, j integer, k integer,
                                               out baseCell integer, out cwwRot60 integer )
as
$body$
declare
 faceIjkBaseCells integer [][3][3][2] = array
 [
    [-- face 0
     [
         -- i 0
         [[16, 0], [18, 0], [24, 0]],  -- j 0
         [[33, 0], [30, 0], [32, 3]],  -- j 1
         [[49, 1], [48, 3], [50, 3]]   -- j 2
     ],
     [
         -- i 1
         [[8, 0], [5, 5], [10, 5]],    -- j 0
         [[22, 0], [16, 0], [18, 0]],  -- j 1
         [[41, 1], [33, 0], [30, 0]]   -- j 2
     ],
     [
         -- i 2
         [[4, 0], [0, 5], [2, 5]],    -- j 0
         [[15, 1], [8, 0], [5, 5]],   -- j 1
         [[31, 1], [22, 0], [16, 0]]  -- j 2
     ]],
    [-- face 1
     [
         -- i 0
         [[2, 0], [6, 0], [14, 0]],    -- j 0
         [[10, 0], [11, 0], [17, 3]],  -- j 1
         [[24, 1], [23, 3], [25, 3]]   -- j 2
     ],
     [
         -- i 1
         [[0, 0], [1, 5], [9, 5]],    -- j 0
         [[5, 0], [2, 0], [6, 0]],    -- j 1
         [[18, 1], [10, 0], [11, 0]]  -- j 2
     ],
     [
         -- i 2
         [[4, 1], [3, 5], [7, 5]],  -- j 0
         [[8, 1], [0, 0], [1, 5]],  -- j 1
         [[16, 1], [5, 0], [2, 0]]  -- j 2
     ]],
    [-- face 2
     [
         -- i 0
         [[7, 0], [21, 0], [38, 0]],  -- j 0
         [[9, 0], [19, 0], [34, 3]],  -- j 1
         [[14, 1], [20, 3], [36, 3]]  -- j 2
     ],
     [
         -- i 1
         [[3, 0], [13, 5], [29, 5]],  -- j 0
         [[1, 0], [7, 0], [21, 0]],   -- j 1
         [[6, 1], [9, 0], [19, 0]]    -- j 2
     ],
     [
         -- i 2
         [[4, 2], [12, 5], [26, 5]],  -- j 0
         [[0, 1], [3, 0], [13, 5]],   -- j 1
         [[2, 1], [1, 0], [7, 0]]     -- j 2
     ]],
    [-- face 3
     [
         -- i 0
         [[26, 0], [42, 0], [58, 0]],  -- j 0
         [[29, 0], [43, 0], [62, 3]],  -- j 1
         [[38, 1], [47, 3], [64, 3]]   -- j 2
     ],
     [
         -- i 1
         [[12, 0], [28, 5], [44, 5]],  -- j 0
         [[13, 0], [26, 0], [42, 0]],  -- j 1
         [[21, 1], [29, 0], [43, 0]]   -- j 2
     ],
     [
         -- i 2
         [[4, 3], [15, 5], [31, 5]],  -- j 0
         [[3, 1], [12, 0], [28, 5]],  -- j 1
         [[7, 1], [13, 0], [26, 0]]   -- j 2
     ]],
    [-- face 4
     [
         -- i 0
         [[31, 0], [41, 0], [49, 0]],  -- j 0
         [[44, 0], [53, 0], [61, 3]],  -- j 1
         [[58, 1], [65, 3], [75, 3]]   -- j 2
     ],
     [
         -- i 1
         [[15, 0], [22, 5], [33, 5]],  -- j 0
         [[28, 0], [31, 0], [41, 0]],  -- j 1
         [[42, 1], [44, 0], [53, 0]]   -- j 2
     ],
     [
         -- i 2
         [[4, 4], [8, 5], [16, 5]],    -- j 0
         [[12, 1], [15, 0], [22, 5]],  -- j 1
         [[26, 1], [28, 0], [31, 0]]   -- j 2
     ]],
    [-- face 5
     [
         -- i 0
         [[50, 0], [48, 0], [49, 3]],  -- j 0
         [[32, 0], [30, 3], [33, 3]],  -- j 1
         [[24, 3], [18, 3], [16, 3]]   -- j 2
     ],
     [
         -- i 1
         [[70, 0], [67, 0], [66, 3]],  -- j 0
         [[52, 3], [50, 0], [48, 0]],  -- j 1
         [[37, 3], [32, 0], [30, 3]]   -- j 2
     ],
     [
         -- i 2
         [[83, 0], [87, 3], [85, 3]],  -- j 0
         [[74, 3], [70, 0], [67, 0]],  -- j 1
         [[57, 1], [52, 3], [50, 0]]   -- j 2
     ]],
    [-- face 6
     [
         -- i 0
         [[25, 0], [23, 0], [24, 3]],  -- j 0
         [[17, 0], [11, 3], [10, 3]],  -- j 1
         [[14, 3], [6, 3], [2, 3]]     -- j 2
     ],
     [
         -- i 1
         [[45, 0], [39, 0], [37, 3]],  -- j 0
         [[35, 3], [25, 0], [23, 0]],  -- j 1
         [[27, 3], [17, 0], [11, 3]]   -- j 2
     ],
     [
         -- i 2
         [[63, 0], [59, 3], [57, 3]],  -- j 0
         [[56, 3], [45, 0], [39, 0]],  -- j 1
         [[46, 3], [35, 3], [25, 0]]   -- j 2
     ]],
    [-- face 7
     [
         -- i 0
         [[36, 0], [20, 0], [14, 3]],  -- j 0
         [[34, 0], [19, 3], [9, 3]],   -- j 1
         [[38, 3], [21, 3], [7, 3]]    -- j 2
     ],
     [
         -- i 1
         [[55, 0], [40, 0], [27, 3]],  -- j 0
         [[54, 3], [36, 0], [20, 0]],  -- j 1
         [[51, 3], [34, 0], [19, 3]]   -- j 2
     ],
     [
         -- i 2
         [[72, 0], [60, 3], [46, 3]],  -- j 0
         [[73, 3], [55, 0], [40, 0]],  -- j 1
         [[71, 3], [54, 3], [36, 0]]   -- j 2
     ]],
    [-- face 8
     [
         -- i 0
         [[64, 0], [47, 0], [38, 3]],  -- j 0
         [[62, 0], [43, 3], [29, 3]],  -- j 1
         [[58, 3], [42, 3], [26, 3]]   -- j 2
     ],
     [
         -- i 1
         [[84, 0], [69, 0], [51, 3]],  -- j 0
         [[82, 3], [64, 0], [47, 0]],  -- j 1
         [[76, 3], [62, 0], [43, 3]]   -- j 2
     ],
     [
         -- i 2
         [[97, 0], [89, 3], [71, 3]],  -- j 0
         [[98, 3], [84, 0], [69, 0]],  -- j 1
         [[96, 3], [82, 3], [64, 0]]   -- j 2
     ]],
    [-- face 9
     [
         -- i 0
         [[75, 0], [65, 0], [58, 3]],  -- j 0
         [[61, 0], [53, 3], [44, 3]],  -- j 1
         [[49, 3], [41, 3], [31, 3]]   -- j 2
     ],
     [
         -- i 1
         [[94, 0], [86, 0], [76, 3]],  -- j 0
         [[81, 3], [75, 0], [65, 0]],  -- j 1
         [[66, 3], [61, 0], [53, 3]]   -- j 2
     ],
     [
         -- i 2
         [[107, 0], [104, 3], [96, 3]],  -- j 0
         [[101, 3], [94, 0], [86, 0]],   -- j 1
         [[85, 3], [81, 3], [75, 0]]     -- j 2
     ]],
    [-- face 10
     [
         -- i 0
         [[57, 0], [59, 0], [63, 3]],  -- j 0
         [[74, 0], [78, 3], [79, 3]],  -- j 1
         [[83, 3], [92, 3], [95, 3]]   -- j 2
     ],
     [
         -- i 1
         [[37, 0], [39, 3], [45, 3]],  -- j 0
         [[52, 0], [57, 0], [59, 0]],  -- j 1
         [[70, 3], [74, 0], [78, 3]]   -- j 2
     ],
     [
         -- i 2
         [[24, 0], [23, 3], [25, 3]],  -- j 0
         [[32, 3], [37, 0], [39, 3]],  -- j 1
         [[50, 3], [52, 0], [57, 0]]   -- j 2
     ]],
    [-- face 11
     [
         -- i 0
         [[46, 0], [60, 0], [72, 3]],  -- j 0
         [[56, 0], [68, 3], [80, 3]],  -- j 1
         [[63, 3], [77, 3], [90, 3]]   -- j 2
     ],
     [
         -- i 1
         [[27, 0], [40, 3], [55, 3]],  -- j 0
         [[35, 0], [46, 0], [60, 0]],  -- j 1
         [[45, 3], [56, 0], [68, 3]]   -- j 2
     ],
     [
         -- i 2
         [[14, 0], [20, 3], [36, 3]],  -- j 0
         [[17, 3], [27, 0], [40, 3]],  -- j 1
         [[25, 3], [35, 0], [46, 0]]   -- j 2
     ]],
    [-- face 12
     [
         -- i 0
         [[71, 0], [89, 0], [97, 3]],   -- j 0
         [[73, 0], [91, 3], [103, 3]],  -- j 1
         [[72, 3], [88, 3], [105, 3]]   -- j 2
     ],
     [
         -- i 1
         [[51, 0], [69, 3], [84, 3]],  -- j 0
         [[54, 0], [71, 0], [89, 0]],  -- j 1
         [[55, 3], [73, 0], [91, 3]]   -- j 2
     ],
     [
         -- i 2
         [[38, 0], [47, 3], [64, 3]],  -- j 0
         [[34, 3], [51, 0], [69, 3]],  -- j 1
         [[36, 3], [54, 0], [71, 0]]   -- j 2
     ]],
    [-- face 13
     [
         -- i 0
         [[96, 0], [104, 0], [107, 3]],  -- j 0
         [[98, 0], [110, 3], [115, 3]],  -- j 1
         [[97, 3], [111, 3], [119, 3]]   -- j 2
     ],
     [
         -- i 1
         [[76, 0], [86, 3], [94, 3]],   -- j 0
         [[82, 0], [96, 0], [104, 0]],  -- j 1
         [[84, 3], [98, 0], [110, 3]]   -- j 2
     ],
     [
         -- i 2
         [[58, 0], [65, 3], [75, 3]],  -- j 0
         [[62, 3], [76, 0], [86, 3]],  -- j 1
         [[64, 3], [82, 0], [96, 0]]   -- j 2
     ]],
    [-- face 14
     [
         -- i 0
         [[85, 0], [87, 0], [83, 3]],     -- j 0
         [[101, 0], [102, 3], [100, 3]],  -- j 1
         [[107, 3], [112, 3], [114, 3]]   -- j 2
     ],
     [
         -- i 1
         [[66, 0], [67, 3], [70, 3]],   -- j 0
         [[81, 0], [85, 0], [87, 0]],   -- j 1
         [[94, 3], [101, 0], [102, 3]]  -- j 2
     ],
     [
         -- i 2
         [[49, 0], [48, 3], [50, 3]],  -- j 0
         [[61, 3], [66, 0], [67, 3]],  -- j 1
         [[75, 3], [81, 0], [85, 0]]   -- j 2
     ]],
    [-- face 15
     [
         -- i 0
         [[95, 0], [92, 0], [83, 0]],  -- j 0
         [[79, 0], [78, 0], [74, 3]],  -- j 1
         [[63, 1], [59, 3], [57, 3]]   -- j 2
     ],
     [
         -- i 1
         [[109, 0], [108, 0], [100, 5]],  -- j 0
         [[93, 1], [95, 0], [92, 0]],     -- j 1
         [[77, 1], [79, 0], [78, 0]]      -- j 2
     ],
     [
         -- i 2
         [[117, 4], [118, 5], [114, 5]],  -- j 0
         [[106, 1], [109, 0], [108, 0]],  -- j 1
         [[90, 1], [93, 1], [95, 0]]      -- j 2
     ]],
    [-- face 16
     [
         -- i 0
         [[90, 0], [77, 0], [63, 0]],  -- j 0
         [[80, 0], [68, 0], [56, 3]],  -- j 1
         [[72, 1], [60, 3], [46, 3]]   -- j 2
     ],
     [
         -- i 1
         [[106, 0], [93, 0], [79, 5]],  -- j 0
         [[99, 1], [90, 0], [77, 0]],   -- j 1
         [[88, 1], [80, 0], [68, 0]]    -- j 2
     ],
     [
         -- i 2
         [[117, 3], [109, 5], [95, 5]],  -- j 0
         [[113, 1], [106, 0], [93, 0]],  -- j 1
         [[105, 1], [99, 1], [90, 0]]    -- j 2
     ]],
    [-- face 17
     [
         -- i 0
         [[105, 0], [88, 0], [72, 0]],  -- j 0
         [[103, 0], [91, 0], [73, 3]],  -- j 1
         [[97, 1], [89, 3], [71, 3]]    -- j 2
     ],
     [
         -- i 1
         [[113, 0], [99, 0], [80, 5]],   -- j 0
         [[116, 1], [105, 0], [88, 0]],  -- j 1
         [[111, 1], [103, 0], [91, 0]]   -- j 2
     ],
     [
         -- i 2
         [[117, 2], [106, 5], [90, 5]],  -- j 0
         [[121, 1], [113, 0], [99, 0]],  -- j 1
         [[119, 1], [116, 1], [105, 0]]  -- j 2
     ]],
    [-- face 18
     [
         -- i 0
         [[119, 0], [111, 0], [97, 0]],  -- j 0
         [[115, 0], [110, 0], [98, 3]],  -- j 1
         [[107, 1], [104, 3], [96, 3]]   -- j 2
     ],
     [
         -- i 1
         [[121, 0], [116, 0], [103, 5]],  -- j 0
         [[120, 1], [119, 0], [111, 0]],  -- j 1
         [[112, 1], [115, 0], [110, 0]]   -- j 2
     ],
     [
         -- i 2
         [[117, 1], [113, 5], [105, 5]],  -- j 0
         [[118, 1], [121, 0], [116, 0]],  -- j 1
         [[114, 1], [120, 1], [119, 0]]   -- j 2
     ]],
    [-- face 19
     [
         -- i 0
         [[114, 0], [112, 0], [107, 0]],  -- j 0
         [[100, 0], [102, 0], [101, 3]],  -- j 1
         [[83, 1], [87, 3], [85, 3]]      -- j 2
     ],
     [
         -- i 1
         [[118, 0], [120, 0], [115, 5]],  -- j 0
         [[108, 1], [114, 0], [112, 0]],  -- j 1
         [[92, 1], [100, 0], [102, 0]]    -- j 2
     ],
     [
         -- i 2
         [[117, 0], [121, 5], [119, 5]],  -- j 0
         [[109, 1], [118, 0], [120, 0]],  -- j 1
         [[95, 1], [108, 1], [114, 0]]    -- j 2
     ]]
];
begin
 i = i+1; j = j+1; k = k+1; icoface = icoface+1;

 baseCell = faceIjkBaseCells[icoface][i][j][k][1];
 cwwRot60 = faceIjkBaseCells[icoface][i][j][k][2];
end;
$body$
language plpgsql immutable;

create or replace function _faceIjkToBaseCell(icoface integer, i integer, j integer, k integer )
returns integer as
$body$
 select (_s_faceIjkBaseCells(icoface,i,j,k)).baseCell
$body$ 
language sql immutable;

create or replace function _faceIjkToBaseCellCCWrot60(icoface integer, i integer, j integer, k integer )
returns integer as
$body$
 select (_s_faceIjkBaseCells(icoface,i,j,k)).cwwRot60
$body$ 
language sql immutable;

create or replace function baseCellData( baseCell integer, 
                                         out face integer, out i integer, out j integer, out k integer, out pentagon boolean,
                                         out cwOffsetPent0 integer, out cwOffsetPent1 integer )
as
$body$
declare
 arrBaseCellData integer [][7] = array
[
    [1, 1, 0, 0, 0, 0, 0],     -- base cell 0
    [2, 1, 1, 0, 0, 0, 0],     -- base cell 1
    [1, 0, 0, 0, 0, 0, 0],     -- base cell 2
    [2, 1, 0, 0, 0, 0, 0],     -- base cell 3
    [0, 2, 0, 0, 1, -1, -1],   -- base cell 4
    [1, 1, 1, 0, 0, 0, 0],     -- base cell 5
    [1, 0, 0, 1, 0, 0, 0],     -- base cell 6
    [2, 0, 0, 0, 0, 0, 0],     -- base cell 7
    [0, 1, 0, 0, 0, 0, 0],     -- base cell 8
    [2, 0, 1, 0, 0, 0, 0],     -- base cell 9
    [1, 0, 1, 0, 0, 0, 0],     -- base cell 10
    [1, 0, 1, 1, 0, 0, 0],     -- base cell 11
    [3, 1, 0, 0, 0, 0, 0],     -- base cell 12
    [3, 1, 1, 0, 0, 0, 0],     -- base cell 13
    [11, 2, 0, 0, 1, 2, 6],    -- base cell 14
    [4, 1, 0, 0, 0, 0, 0],     -- base cell 15
    [0, 0, 0, 0, 0, 0, 0],     -- base cell 16
    [6, 0, 1, 0, 0, 0, 0],     -- base cell 17
    [0, 0, 0, 1, 0, 0, 0],     -- base cell 18
    [2, 0, 1, 1, 0, 0, 0],     -- base cell 19
    [7, 0, 0, 1, 0, 0, 0],     -- base cell 20
    [2, 0, 0, 1, 0, 0, 0],     -- base cell 21
    [0, 1, 1, 0, 0, 0, 0],     -- base cell 22
    [6, 0, 0, 1, 0, 0, 0],     -- base cell 23
    [10, 2, 0, 0, 1, 1, 5],    -- base cell 24
    [6, 0, 0, 0, 0, 0, 0],     -- base cell 25
    [3, 0, 0, 0, 0, 0, 0],     -- base cell 26
    [11, 1, 0, 0, 0, 0, 0],    -- base cell 27
    [4, 1, 1, 0, 0, 0, 0],     -- base cell 28
    [3, 0, 1, 0, 0, 0, 0],     -- base cell 29
    [0, 0, 1, 1, 0, 0, 0],     -- base cell 30
    [4, 0, 0, 0, 0, 0, 0],     -- base cell 31
    [5, 0, 1, 0, 0, 0, 0],     -- base cell 32
    [0, 0, 1, 0, 0, 0, 0],     -- base cell 33
    [7, 0, 1, 0, 0, 0, 0],     -- base cell 34
    [11, 1, 1, 0, 0, 0, 0],    -- base cell 35
    [7, 0, 0, 0, 0, 0, 0],     -- base cell 36
    [10, 1, 0, 0, 0, 0, 0],    -- base cell 37
    [12, 2, 0, 0, 1, 3, 7],    -- base cell 38
    [6, 1, 0, 1, 0, 0, 0],     -- base cell 39
    [7, 1, 0, 1, 0, 0, 0],     -- base cell 40
    [4, 0, 0, 1, 0, 0, 0],     -- base cell 41
    [3, 0, 0, 1, 0, 0, 0],     -- base cell 42
    [3, 0, 1, 1, 0, 0, 0],     -- base cell 43
    [4, 0, 1, 0, 0, 0, 0],     -- base cell 44
    [6, 1, 0, 0, 0, 0, 0],     -- base cell 45
    [11, 0, 0, 0, 0, 0, 0],    -- base cell 46
    [8, 0, 0, 1, 0, 0, 0],     -- base cell 47
    [5, 0, 0, 1, 0, 0, 0],     -- base cell 48
    [14, 2, 0, 0, 1, 0, 9],    -- base cell 49
    [5, 0, 0, 0, 0, 0, 0],     -- base cell 50
    [12, 1, 0, 0, 0, 0, 0],    -- base cell 51
    [10, 1, 1, 0, 0, 0, 0],    -- base cell 52
    [4, 0, 1, 1, 0, 0, 0],     -- base cell 53
    [12, 1, 1, 0, 0, 0, 0],    -- base cell 54
    [7, 1, 0, 0, 0, 0, 0],     -- base cell 55
    [11, 0, 1, 0, 0, 0, 0],    -- base cell 56
    [10, 0, 0, 0, 0, 0, 0],    -- base cell 57
    [13, 2, 0, 0, 1, 4, 8],    -- base cell 58
    [10, 0, 0, 1, 0, 0, 0],    -- base cell 59
    [11, 0, 0, 1, 0, 0, 0],    -- base cell 60
    [9, 0, 1, 0, 0, 0, 0],     -- base cell 61
    [8, 0, 1, 0, 0, 0, 0],     -- base cell 62
    [6, 2, 0, 0, 1, 11, 15],   -- base cell 63
    [8, 0, 0, 0, 0, 0, 0],     -- base cell 64
    [9, 0, 0, 1, 0, 0, 0],     -- base cell 65
    [14, 1, 0, 0, 0, 0, 0],    -- base cell 66
    [5, 1, 0, 1, 0, 0, 0],     -- base cell 67
    [16, 0, 1, 1, 0, 0, 0],    -- base cell 68
    [8, 1, 0, 1, 0, 0, 0],     -- base cell 69
    [5, 1, 0, 0, 0, 0, 0],     -- base cell 70
    [12, 0, 0, 0, 0, 0, 0],    -- base cell 71
    [7, 2, 0, 0, 1, 12, 16],   -- base cell 72
    [12, 0, 1, 0, 0, 0, 0],    -- base cell 73
    [10, 0, 1, 0, 0, 0, 0],    -- base cell 74
    [9, 0, 0, 0, 0, 0, 0],     -- base cell 75
    [13, 1, 0, 0, 0, 0, 0],    -- base cell 76
    [16, 0, 0, 1, 0, 0, 0],    -- base cell 77
    [15, 0, 1, 1, 0, 0, 0],    -- base cell 78
    [15, 0, 1, 0, 0, 0, 0],    -- base cell 79
    [16, 0, 1, 0, 0, 0, 0],    -- base cell 80
    [14, 1, 1, 0, 0, 0, 0],    -- base cell 81
    [13, 1, 1, 0, 0, 0, 0],    -- base cell 82
    [5, 2, 0, 0, 1, 10, 19],   -- base cell 83
    [8, 1, 0, 0, 0, 0, 0],     -- base cell 84
    [14, 0, 0, 0, 0, 0, 0],    -- base cell 85
    [9, 1, 0, 1, 0, 0, 0],     -- base cell 86
    [14, 0, 0, 1, 0, 0, 0],    -- base cell 87
    [17, 0, 0, 1, 0, 0, 0],    -- base cell 88
    [12, 0, 0, 1, 0, 0, 0],    -- base cell 89
    [16, 0, 0, 0, 0, 0, 0],    -- base cell 90
    [17, 0, 1, 1, 0, 0, 0],    -- base cell 91
    [15, 0, 0, 1, 0, 0, 0],    -- base cell 92
    [16, 1, 0, 1, 0, 0, 0],    -- base cell 93
    [9, 1, 0, 0, 0, 0, 0],     -- base cell 94
    [15, 0, 0, 0, 0, 0, 0],    -- base cell 95
    [13, 0, 0, 0, 0, 0, 0],    -- base cell 96
    [8, 2, 0, 0, 1, 13, 17],   -- base cell 97
    [13, 0, 1, 0, 0, 0, 0],    -- base cell 98
    [17, 1, 0, 1, 0, 0, 0],    -- base cell 99
    [19, 0, 1, 0, 0, 0, 0],    -- base cell 100
    [14, 0, 1, 0, 0, 0, 0],    -- base cell 101
    [19, 0, 1, 1, 0, 0, 0],    -- base cell 102
    [17, 0, 1, 0, 0, 0, 0],    -- base cell 103
    [13, 0, 0, 1, 0, 0, 0],    -- base cell 104
    [17, 0, 0, 0, 0, 0, 0],    -- base cell 105
    [16, 1, 0, 0, 0, 0, 0],    -- base cell 106
    [9, 2, 0, 0, 1, 14, 18],   -- base cell 107
    [15, 1, 0, 1, 0, 0, 0],    -- base cell 108
    [15, 1, 0, 0, 0, 0, 0],    -- base cell 109
    [18, 0, 1, 1, 0, 0, 0],    -- base cell 110
    [18, 0, 0, 1, 0, 0, 0],    -- base cell 111
    [19, 0, 0, 1, 0, 0, 0],    -- base cell 112
    [17, 1, 0, 0, 0, 0, 0],    -- base cell 113
    [19, 0, 0, 0, 0, 0, 0],    -- base cell 114
    [18, 0, 1, 0, 0, 0, 0],    -- base cell 115
    [18, 1, 0, 1, 0, 0, 0],    -- base cell 116
    [19, 2, 0, 0, 1, -1, -1],  -- base cell 117
    [19, 1, 0, 0, 0, 0, 0],    -- base cell 118
    [18, 0, 0, 0, 0, 0, 0],    -- base cell 119
    [19, 1, 0, 1, 0, 0, 0],    -- base cell 120
    [18, 1, 0, 0, 0, 0, 0]     -- base cell 121
];
begin
 
 baseCell = baseCell+1;
 
 face     = arrBaseCellData[baseCell][1];
 i        = arrBaseCellData[baseCell][2];
 j        = arrBaseCellData[baseCell][3];
 k        = arrBaseCellData[baseCell][4];
 pentagon = (arrBaseCellData[baseCell][5] != 0);
 cwOffsetPent0 = arrBaseCellData[baseCell][6];
 cwOffsetPent1 = arrBaseCellData[baseCell][7];
end;
$body$
language plpgsql immutable;

create or replace function _isBaseCellPentagon( baseCell integer) 
 returns boolean as 
$body$ 
declare
 rBaseCell record;
begin
 rBaseCell = baseCellData( baseCell ); 
 return rBaseCell.pentagon;
end;
$body$
language plpgsql immutable;

create or replace function _isBaseCellPolarPentagon(baseCell integer) 
 returns boolean as 
$body$ 
 select (baseCell = 4) or (baseCell = 117)
$body$ 
language sql immutable;

create or replace function _baseCellIsCwOffset( baseCell integer, testFace integer) 
 returns boolean as 
$body$ 
declare
 rBaseCell record;
begin
 rBaseCell = baseCellData( baseCell ); 
 return (( rBaseCell.cwOffsetPent0 = testFace ) or ( rBaseCell.cwOffsetPent1 = testFace ));
end;
$body$
language plpgsql immutable;

create or replace function maxDimByCIIres( res integer  )
 returns integer as
$body$
declare
 arrDim integer [] = array
[
        2, -- res  0
       -1, -- res  1
       14, -- res  2
       -1, -- res  3
       98, -- res  4
       -1, -- res  5
      686, -- res  6
       -1, -- res  7
     4802, -- res  8
       -1, -- res  9
    33614, -- res 10
       -1, -- res 11
   235298, -- res 12
       -1, -- res 13
  1647086, -- res 14
       -1, -- res 15
  11529602 -- res 16
];
begin
 
 res = res + 1;
 return arrDim[res]; 

end;
$body$
language plpgsql immutable;

create or replace function unitScaleByCIIres( res integer  )
 returns integer as
$body$
declare
 arrScale integer [] = array
[
    1,       -- res  0
    -1,      -- res  1
    7,       -- res  2
    -1,      -- res  3
    49,      -- res  4
    -1,      -- res  5
    343,     -- res  6
    -1,      -- res  7
    2401,    -- res  8
    -1,      -- res  9
    16807,   -- res 10
    -1,      -- res 11
    117649,  -- res 12
    -1,      -- res 13
    823543,  -- res 14
    -1,      -- res 15
    5764801  -- res 16
];
begin
 
 res = res + 1;
 return arrScale[res]; 

end;
$body$
language plpgsql immutable;


create or replace function faceNeighbors(icosa_face_nr integer, quadrant integer, 
                                         out face integer, 
                                         out i integer, out j integer, out k integer,
                                         out ccwrot60 integer)
as
$body$
declare
 arr_face_neigh integer [][4][5] = array
[
    [
        -- face 0
        [0, 0, 0, 0, 0],  -- central face
        [4, 2, 0, 2, 1],  -- ij quadrant
        [1, 2, 2, 0, 5],  -- ki quadrant
        [5, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 1
        [1, 0, 0, 0, 0],  -- central face
        [0, 2, 0, 2, 1],  -- ij quadrant
        [2, 2, 2, 0, 5],  -- ki quadrant
        [6, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 2
        [2, 0, 0, 0, 0],  -- central face
        [1, 2, 0, 2, 1],  -- ij quadrant
        [3, 2, 2, 0, 5],  -- ki quadrant
        [7, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 3
        [3, 0, 0, 0, 0],  -- central face
        [2, 2, 0, 2, 1],  -- ij quadrant
        [4, 2, 2, 0, 5],  -- ki quadrant
        [8, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 4
        [4, 0, 0, 0, 0],  -- central face
        [3, 2, 0, 2, 1],  -- ij quadrant
        [0, 2, 2, 0, 5],  -- ki quadrant
        [9, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 5
        [ 5, 0, 0, 0, 0],  -- central face
        [10, 2, 2, 0, 3],  -- ij quadrant
        [14, 2, 0, 2, 3],  -- ki quadrant
        [ 0, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 6
        [ 6, 0, 0, 0, 0],  -- central face
        [11, 2, 2, 0, 3],  -- ij quadrant
        [10, 2, 0, 2, 3],  -- ki quadrant
        [ 1, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 7
        [ 7, 0, 0, 0, 0],  -- central face
        [12, 2, 2, 0, 3],  -- ij quadrant
        [11, 2, 0, 2, 3],  -- ki quadrant
        [ 2, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 8
        [ 8, 0, 0, 0, 0],  -- central face
        [13, 2, 2, 0, 3],  -- ij quadrant
        [12, 2, 0, 2, 3],  -- ki quadrant
        [ 3, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 9
        [ 9, 0, 0, 0, 0],  -- central face
        [14, 2, 2, 0, 3],  -- ij quadrant
        [13, 2, 0, 2, 3],  -- ki quadrant
        [ 4, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 10
        [10, 0, 0, 0, 0],  -- central face
        [ 5, 2, 2, 0, 3],  -- ij quadrant
        [ 6, 2, 0, 2, 3],  -- ki quadrant
        [15, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 11
        [11, 0, 0, 0, 0],  -- central face
        [ 6, 2, 2, 0, 3],  -- ij quadrant
        [ 7, 2, 0, 2, 3],  -- ki quadrant
        [16, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 12
        [12, 0, 0, 0, 0],  -- central face
        [ 7, 2, 2, 0, 3],  -- ij quadrant
        [ 8, 2, 0, 2, 3],  -- ki quadrant
        [17, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 13
        [13, 0, 0, 0, 0],  -- central face
        [ 8, 2, 2, 0, 3],  -- ij quadrant
        [ 9, 2, 0, 2, 3],  -- ki quadrant
        [18, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 14
        [14, 0, 0, 0, 0],  -- central face
        [ 9, 2, 2, 0, 3],  -- ij quadrant
        [ 5, 2, 0, 2, 3],  -- ki quadrant
        [19, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 15
        [15, 0, 0, 0, 0],  -- central face
        [16, 2, 0, 2, 1],  -- ij quadrant
        [19, 2, 2, 0, 5],  -- ki quadrant
        [10, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 16
        [16, 0, 0, 0, 0],  -- central face
        [17, 2, 0, 2, 1],  -- ij quadrant
        [15, 2, 2, 0, 5],  -- ki quadrant
        [11, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 17
        [17, 0, 0, 0, 0],  -- central face
        [18, 2, 0, 2, 1],  -- ij quadrant
        [16, 2, 2, 0, 5],  -- ki quadrant
        [12, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 18
        [18, 0, 0, 0, 0],  -- central face
        [19, 2, 0, 2, 1],  -- ij quadrant
        [17, 2, 2, 0, 5],  -- ki quadrant
        [13, 0, 2, 2, 3]   -- jk quadrant
    ],
    [
        -- face 19
        [19, 0, 0, 0, 0],  -- central face
        [15, 2, 0, 2, 1],  -- ij quadrant
        [18, 2, 2, 0, 5],  -- ki quadrant
        [14, 0, 2, 2, 3]   -- jk quadrant
    ]
];
begin
 icosa_face_nr = icosa_face_nr + 1;
 quadrant = quadrant + 1;

 face     = arr_face_neigh[icosa_face_nr][quadrant][1];
 i        = arr_face_neigh[icosa_face_nr][quadrant][2];
 j        = arr_face_neigh[icosa_face_nr][quadrant][3];
 k        = arr_face_neigh[icosa_face_nr][quadrant][4];
 ccwrot60 = arr_face_neigh[icosa_face_nr][quadrant][5];

end;
$body$
language plpgsql immutable;

create or replace function adjacentFaceDir( origin integer, destination integer  )
 returns integer as
$body$
declare
 arr_adjacent_face_dir integer[20][20] = array
[ -- IJ = 1, KI = 2, JK = 3 
    [0,  2 /*KI*/, -1, -1, 1 /*IJ*/, 3 /*JK*/, -1, -1, -1, -1,
     -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 0
    [1 /*IJ*/, 0,  2 /*KI*/, -1, -1, -1, 3 /*JK*/, -1, -1, -1,
     -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 1
    [-1, 1 /*IJ*/, 0,  2 /*KI*/, -1, -1, -1, 3 /*JK*/, -1, -1,
     -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 2
    [-1, -1, 1 /*IJ*/, 0,  2 /*KI*/, -1, -1, -1, 3 /*JK*/, -1,
     -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 3
    [2 /*KI*/, -1, -1, 1 /*IJ*/, 0,  -1, -1, -1, -1, 3 /*JK*/,
     -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 4
    [3 /*JK*/, -1, -1, -1, -1, 0,  -1, -1, -1, -1,
     1 /*IJ*/, -1, -1, -1, 2 /*KI*/, -1, -1, -1, -1, -1],  -- face 5
    [-1, 3 /*JK*/, -1, -1, -1, -1, 0,  -1, -1, -1,
     2 /*KI*/, 1 /*IJ*/, -1, -1, -1, -1, -1, -1, -1, -1],  -- face 6
    [-1, -1, 3 /*JK*/, -1, -1, -1, -1, 0,  -1, -1,
     -1, 2 /*KI*/, 1 /*IJ*/, -1, -1, -1, -1, -1, -1, -1],  -- face 7
    [-1, -1, -1, 3 /*JK*/, -1, -1, -1, -1, 0,  -1,
     -1, -1, 2 /*KI*/, 1 /*IJ*/, -1, -1, -1, -1, -1, -1],  -- face 8
    [-1, -1, -1, -1, 3 /*JK*/, -1, -1, -1, -1, 0,
     -1, -1, -1, 2 /*KI*/, 1 /*IJ*/, -1, -1, -1, -1, -1],  -- face 9
    [-1, -1, -1, -1, -1, 1 /*IJ*/, 2 /*KI*/, -1, -1, -1,
     0,  -1, -1, -1, -1, 3 /*JK*/, -1, -1, -1, -1],  -- face 10
    [-1, -1, -1, -1, -1, -1, 1 /*IJ*/, 2 /*KI*/, -1, -1,
     -1, 0,  -1, -1, -1, -1, 3 /*JK*/, -1, -1, -1],  -- face 11
    [-1, -1, -1, -1, -1, -1, -1, 1 /*IJ*/, 2 /*KI*/, -1,
     -1, -1, 0,  -1, -1, -1, -1, 3 /*JK*/, -1, -1],  -- face 12
    [-1, -1, -1, -1, -1, -1, -1, -1, 1 /*IJ*/, 2 /*KI*/,
     -1, -1, -1, 0,  -1, -1, -1, -1, 3 /*JK*/, -1],  -- face 13
    [-1, -1, -1, -1, -1, 2 /*KI*/, -1, -1, -1, 1 /*IJ*/,
     -1, -1, -1, -1, 0,  -1, -1, -1, -1, 3 /*JK*/],  -- face 14
    [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     3 /*JK*/, -1, -1, -1, -1, 0,  1 /*IJ*/, -1, -1, 2 /*KI*/],  -- face 15
    [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     -1, 3 /*JK*/, -1, -1, -1, 2 /*KI*/, 0,  1 /*IJ*/, -1, -1],  -- face 16
    [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     -1, -1, 3 /*JK*/, -1, -1, -1, 2 /*KI*/, 0,  1 /*IJ*/, -1],  -- face 17
    [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     -1, -1, -1, 3 /*JK*/, -1, -1, -1, 2 /*KI*/, 0,  1 /*IJ*/],  -- face 18
    [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     -1, -1, -1, -1, 3 /*JK*/, 1 /*IJ*/, -1, -1, 2 /*KI*/, 0]  -- face 19
];
begin
 return arr_adjacent_face_dir[origin +1][destination +1]; 
end;
$body$
language plpgsql immutable;

create or replace function baseCellNeighbors( basecell integer, neighbor integer  )
 returns integer as
$body$
declare
arr_baseCellNeighbors integer [ 122 /*NUM_BASE_CELLS*/][7] = array
[
    [0, 1, 5, 2, 4, 3, 8],                          -- base cell 0
    [1, 7, 6, 9, 0, 3, 2],                          -- base cell 1
    [2, 6, 10, 11, 0, 1, 5],                        -- base cell 2
    [3, 13, 1, 7, 4, 12, 0],                        -- base cell 3
    [4, 127 /*INVALID_BASE_CELL*/, 15, 8, 3, 0, 12],        -- base cell 4 (pentagon)
    [5, 2, 18, 10, 8, 0, 16],                       -- base cell 5
    [6, 14, 11, 17, 1, 9, 2],                       -- base cell 6
    [7, 21, 9, 19, 3, 13, 1],                       -- base cell 7
    [8, 5, 22, 16, 4, 0, 15],                       -- base cell 8
    [9, 19, 14, 20, 1, 7, 6],                       -- base cell 9
    [10, 11, 24, 23, 5, 2, 18],                     -- base cell 10
    [11, 17, 23, 25, 2, 6, 10],                     -- base cell 11
    [12, 28, 13, 26, 4, 15, 3],                     -- base cell 12
    [13, 26, 21, 29, 3, 12, 7],                     -- base cell 13
    [14, 127 /*INVALID_BASE_CELL*/, 17, 27, 9, 20, 6],      -- base cell 14 (pentagon)
    [15, 22, 28, 31, 4, 8, 12],                     -- base cell 15
    [16, 18, 33, 30, 8, 5, 22],                     -- base cell 16
    [17, 11, 14, 6, 35, 25, 27],                    -- base cell 17
    [18, 24, 30, 32, 5, 10, 16],                    -- base cell 18
    [19, 34, 20, 36, 7, 21, 9],                     -- base cell 19
    [20, 14, 19, 9, 40, 27, 36],                    -- base cell 20
    [21, 38, 19, 34, 13, 29, 7],                    -- base cell 21
    [22, 16, 41, 33, 15, 8, 31],                    -- base cell 22
    [23, 24, 11, 10, 39, 37, 25],                   -- base cell 23
    [24, 127 /*INVALID_BASE_CELL*/, 32, 37, 10, 23, 18],    -- base cell 24 (pentagon)
    [25, 23, 17, 11, 45, 39, 35],                   -- base cell 25
    [26, 42, 29, 43, 12, 28, 13],                   -- base cell 26
    [27, 40, 35, 46, 14, 20, 17],                   -- base cell 27
    [28, 31, 42, 44, 12, 15, 26],                   -- base cell 28
    [29, 43, 38, 47, 13, 26, 21],                   -- base cell 29
    [30, 32, 48, 50, 16, 18, 33],                   -- base cell 30
    [31, 41, 44, 53, 15, 22, 28],                   -- base cell 31
    [32, 30, 24, 18, 52, 50, 37],                   -- base cell 32
    [33, 30, 49, 48, 22, 16, 41],                   -- base cell 33
    [34, 19, 38, 21, 54, 36, 51],                   -- base cell 34
    [35, 46, 45, 56, 17, 27, 25],                   -- base cell 35
    [36, 20, 34, 19, 55, 40, 54],                   -- base cell 36
    [37, 39, 52, 57, 24, 23, 32],                   -- base cell 37
    [38, 127 /*INVALID_BASE_CELL*/, 34, 51, 29, 47, 21],    -- base cell 38 (pentagon)
    [39, 37, 25, 23, 59, 57, 45],                   -- base cell 39
    [40, 27, 36, 20, 60, 46, 55],                   -- base cell 40
    [41, 49, 53, 61, 22, 33, 31],                   -- base cell 41
    [42, 58, 43, 62, 28, 44, 26],                   -- base cell 42
    [43, 62, 47, 64, 26, 42, 29],                   -- base cell 43
    [44, 53, 58, 65, 28, 31, 42],                   -- base cell 44
    [45, 39, 35, 25, 63, 59, 56],                   -- base cell 45
    [46, 60, 56, 68, 27, 40, 35],                   -- base cell 46
    [47, 38, 43, 29, 69, 51, 64],                   -- base cell 47
    [48, 49, 30, 33, 67, 66, 50],                   -- base cell 48
    [49, 127 /*INVALID_BASE_CELL*/, 61, 66, 33, 48, 41],    -- base cell 49 (pentagon)
    [50, 48, 32, 30, 70, 67, 52],                   -- base cell 50
    [51, 69, 54, 71, 38, 47, 34],                   -- base cell 51
    [52, 57, 70, 74, 32, 37, 50],                   -- base cell 52
    [53, 61, 65, 75, 31, 41, 44],                   -- base cell 53
    [54, 71, 55, 73, 34, 51, 36],                   -- base cell 54
    [55, 40, 54, 36, 72, 60, 73],                   -- base cell 55
    [56, 68, 63, 77, 35, 46, 45],                   -- base cell 56
    [57, 59, 74, 78, 37, 39, 52],                   -- base cell 57
    [58, 127 /*INVALID_BASE_CELL*/, 62, 76, 44, 65, 42],    -- base cell 58 (pentagon)
    [59, 63, 78, 79, 39, 45, 57],                   -- base cell 59
    [60, 72, 68, 80, 40, 55, 46],                   -- base cell 60
    [61, 53, 49, 41, 81, 75, 66],                   -- base cell 61
    [62, 43, 58, 42, 82, 64, 76],                   -- base cell 62
    [63, 127 /*INVALID_BASE_CELL*/, 56, 45, 79, 59, 77],    -- base cell 63 (pentagon)
    [64, 47, 62, 43, 84, 69, 82],                   -- base cell 64
    [65, 58, 53, 44, 86, 76, 75],                   -- base cell 65
    [66, 67, 81, 85, 49, 48, 61],                   -- base cell 66
    [67, 66, 50, 48, 87, 85, 70],                   -- base cell 67
    [68, 56, 60, 46, 90, 77, 80],                   -- base cell 68
    [69, 51, 64, 47, 89, 71, 84],                   -- base cell 69
    [70, 67, 52, 50, 83, 87, 74],                   -- base cell 70
    [71, 89, 73, 91, 51, 69, 54],                   -- base cell 71
    [72, 127 /*INVALID_BASE_CELL*/, 73, 55, 80, 60, 88],    -- base cell 72 (pentagon)
    [73, 91, 72, 88, 54, 71, 55],                   -- base cell 73
    [74, 78, 83, 92, 52, 57, 70],                   -- base cell 74
    [75, 65, 61, 53, 94, 86, 81],                   -- base cell 75
    [76, 86, 82, 96, 58, 65, 62],                   -- base cell 76
    [77, 63, 68, 56, 93, 79, 90],                   -- base cell 77
    [78, 74, 59, 57, 95, 92, 79],                   -- base cell 78
    [79, 78, 63, 59, 93, 95, 77],                   -- base cell 79
    [80, 68, 72, 60, 99, 90, 88],                   -- base cell 80
    [81, 85, 94, 101, 61, 66, 75],                  -- base cell 81
    [82, 96, 84, 98, 62, 76, 64],                   -- base cell 82
    [83, 127 /*INVALID_BASE_CELL*/, 74, 70, 100, 87, 92],   -- base cell 83 (pentagon)
    [84, 69, 82, 64, 97, 89, 98],                   -- base cell 84
    [85, 87, 101, 102, 66, 67, 81],                 -- base cell 85
    [86, 76, 75, 65, 104, 96, 94],                  -- base cell 86
    [87, 83, 102, 100, 67, 70, 85],                 -- base cell 87
    [88, 72, 91, 73, 99, 80, 105],                  -- base cell 88
    [89, 97, 91, 103, 69, 84, 71],                  -- base cell 89
    [90, 77, 80, 68, 106, 93, 99],                  -- base cell 90
    [91, 73, 89, 71, 105, 88, 103],                 -- base cell 91
    [92, 83, 78, 74, 108, 100, 95],                 -- base cell 92
    [93, 79, 90, 77, 109, 95, 106],                 -- base cell 93
    [94, 86, 81, 75, 107, 104, 101],                -- base cell 94
    [95, 92, 79, 78, 109, 108, 93],                 -- base cell 95
    [96, 104, 98, 110, 76, 86, 82],                 -- base cell 96
    [97, 127 /*INVALID_BASE_CELL*/, 98, 84, 103, 89, 111],  -- base cell 97 (pentagon)
    [98, 110, 97, 111, 82, 96, 84],                 -- base cell 98
    [99, 80, 105, 88, 106, 90, 113],                -- base cell 99
    [100, 102, 83, 87, 108, 114, 92],               -- base cell 100
    [101, 102, 107, 112, 81, 85, 94],               -- base cell 101
    [102, 101, 87, 85, 114, 112, 100],              -- base cell 102
    [103, 91, 97, 89, 116, 105, 111],               -- base cell 103
    [104, 107, 110, 115, 86, 94, 96],               -- base cell 104
    [105, 88, 103, 91, 113, 99, 116],               -- base cell 105
    [106, 93, 99, 90, 117, 109, 113],               -- base cell 106
    [107, 127 /*INVALID_BASE_CELL*/, 101, 94, 115, 104, 112], -- base cell 107 (pentagon)
    [108, 100, 95, 92, 118, 114, 109],    -- base cell 108
    [109, 108, 93, 95, 117, 118, 106],    -- base cell 109
    [110, 98, 104, 96, 119, 111, 115],    -- base cell 110
    [111, 97, 110, 98, 116, 103, 119],    -- base cell 111
    [112, 107, 102, 101, 120, 115, 114],  -- base cell 112
    [113, 99, 116, 105, 117, 106, 121],   -- base cell 113
    [114, 112, 100, 102, 118, 120, 108],  -- base cell 114
    [115, 110, 107, 104, 120, 119, 112],  -- base cell 115
    [116, 103, 119, 111, 113, 105, 121],  -- base cell 116
    [117, 127 /*INVALID_BASE_CELL*/, 109, 118, 113, 121, 106], -- base cell 117 (pentagon)
    [118, 120, 108, 114, 117, 121, 109],  -- base cell 118
    [119, 111, 115, 110, 121, 116, 120],  -- base cell 119
    [120, 115, 114, 112, 121, 119, 118],  -- base cell 120
    [121, 116, 120, 119, 117, 113, 118]   -- base cell 121
];
begin
 return arr_baseCellNeighbors[basecell +1][neighbor +1]; 
end;
$body$
language plpgsql immutable;

create or replace function baseCellNeighbor60CCWRots( basecell integer, neighbor integer  )
 returns integer as
$body$
declare
arr_baseCellNeighbor60CCWRots integer[ 122 /*NUM_BASE_CELLS*/][7] = array
[
    [0, 5, 0, 0, 1, 5, 1],   -- base cell 0
    [0, 0, 1, 0, 1, 0, 1],   -- base cell 1
    [0, 0, 0, 0, 0, 5, 0],   -- base cell 2
    [0, 5, 0, 0, 2, 5, 1],   -- base cell 3
    [0, -1, 1, 0, 3, 4, 2],  -- base cell 4 (pentagon)
    [0, 0, 1, 0, 1, 0, 1],   -- base cell 5
    [0, 0, 0, 3, 5, 5, 0],   -- base cell 6
    [0, 0, 0, 0, 0, 5, 0],   -- base cell 7
    [0, 5, 0, 0, 0, 5, 1],   -- base cell 8
    [0, 0, 1, 3, 0, 0, 1],   -- base cell 9
    [0, 0, 1, 3, 0, 0, 1],   -- base cell 10
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 11
    [0, 5, 0, 0, 3, 5, 1],   -- base cell 12
    [0, 0, 1, 0, 1, 0, 1],   -- base cell 13
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 14 (pentagon)
    [0, 5, 0, 0, 4, 5, 1],   -- base cell 15
    [0, 0, 0, 0, 0, 5, 0],   -- base cell 16
    [0, 3, 3, 3, 3, 0, 3],   -- base cell 17
    [0, 0, 0, 3, 5, 5, 0],   -- base cell 18
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 19
    [0, 3, 3, 3, 0, 3, 0],   -- base cell 20
    [0, 0, 0, 3, 5, 5, 0],   -- base cell 21
    [0, 0, 1, 0, 1, 0, 1],   -- base cell 22
    [0, 3, 3, 3, 0, 3, 0],   -- base cell 23
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 24 (pentagon)
    [0, 0, 0, 3, 0, 0, 3],   -- base cell 25
    [0, 0, 0, 0, 0, 5, 0],   -- base cell 26
    [0, 3, 0, 0, 0, 3, 3],   -- base cell 27
    [0, 0, 1, 0, 1, 0, 1],   -- base cell 28
    [0, 0, 1, 3, 0, 0, 1],   -- base cell 29
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 30
    [0, 0, 0, 0, 0, 5, 0],   -- base cell 31
    [0, 3, 3, 3, 3, 0, 3],   -- base cell 32
    [0, 0, 1, 3, 0, 0, 1],   -- base cell 33
    [0, 3, 3, 3, 3, 0, 3],   -- base cell 34
    [0, 0, 3, 0, 3, 0, 3],   -- base cell 35
    [0, 0, 0, 3, 0, 0, 3],   -- base cell 36
    [0, 3, 0, 0, 0, 3, 3],   -- base cell 37
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 38 (pentagon)
    [0, 3, 0, 0, 3, 3, 0],   -- base cell 39
    [0, 3, 0, 0, 3, 3, 0],   -- base cell 40
    [0, 0, 0, 3, 5, 5, 0],   -- base cell 41
    [0, 0, 0, 3, 5, 5, 0],   -- base cell 42
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 43
    [0, 0, 1, 3, 0, 0, 1],   -- base cell 44
    [0, 0, 3, 0, 0, 3, 3],   -- base cell 45
    [0, 0, 0, 3, 0, 3, 0],   -- base cell 46
    [0, 3, 3, 3, 0, 3, 0],   -- base cell 47
    [0, 3, 3, 3, 0, 3, 0],   -- base cell 48
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 49 (pentagon)
    [0, 0, 0, 3, 0, 0, 3],   -- base cell 50
    [0, 3, 0, 0, 0, 3, 3],   -- base cell 51
    [0, 0, 3, 0, 3, 0, 3],   -- base cell 52
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 53
    [0, 0, 3, 0, 3, 0, 3],   -- base cell 54
    [0, 0, 3, 0, 0, 3, 3],   -- base cell 55
    [0, 3, 3, 3, 0, 0, 3],   -- base cell 56
    [0, 0, 0, 3, 0, 3, 0],   -- base cell 57
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 58 (pentagon)
    [0, 3, 3, 3, 3, 3, 0],   -- base cell 59
    [0, 3, 3, 3, 3, 3, 0],   -- base cell 60
    [0, 3, 3, 3, 3, 0, 3],   -- base cell 61
    [0, 3, 3, 3, 3, 0, 3],   -- base cell 62
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 63 (pentagon)
    [0, 0, 0, 3, 0, 0, 3],   -- base cell 64
    [0, 3, 3, 3, 0, 3, 0],   -- base cell 65
    [0, 3, 0, 0, 0, 3, 3],   -- base cell 66
    [0, 3, 0, 0, 3, 3, 0],   -- base cell 67
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 68
    [0, 3, 0, 0, 3, 3, 0],   -- base cell 69
    [0, 0, 3, 0, 0, 3, 3],   -- base cell 70
    [0, 0, 0, 3, 0, 3, 0],   -- base cell 71
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 72 (pentagon)
    [0, 3, 3, 3, 0, 0, 3],   -- base cell 73
    [0, 3, 3, 3, 0, 0, 3],   -- base cell 74
    [0, 0, 0, 3, 0, 0, 3],   -- base cell 75
    [0, 3, 0, 0, 0, 3, 3],   -- base cell 76
    [0, 0, 0, 3, 0, 5, 0],   -- base cell 77
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 78
    [0, 0, 1, 3, 1, 0, 1],   -- base cell 79
    [0, 0, 1, 3, 1, 0, 1],   -- base cell 80
    [0, 0, 3, 0, 3, 0, 3],   -- base cell 81
    [0, 0, 3, 0, 3, 0, 3],   -- base cell 82
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 83 (pentagon)
    [0, 0, 3, 0, 0, 3, 3],   -- base cell 84
    [0, 0, 0, 3, 0, 3, 0],   -- base cell 85
    [0, 3, 0, 0, 3, 3, 0],   -- base cell 86
    [0, 3, 3, 3, 3, 3, 0],   -- base cell 87
    [0, 0, 0, 3, 0, 5, 0],   -- base cell 88
    [0, 3, 3, 3, 3, 3, 0],   -- base cell 89
    [0, 0, 0, 0, 0, 0, 1],   -- base cell 90
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 91
    [0, 0, 0, 3, 0, 5, 0],   -- base cell 92
    [0, 5, 0, 0, 5, 5, 0],   -- base cell 93
    [0, 0, 3, 0, 0, 3, 3],   -- base cell 94
    [0, 0, 0, 0, 0, 0, 1],   -- base cell 95
    [0, 0, 0, 3, 0, 3, 0],   -- base cell 96
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 97 (pentagon)
    [0, 3, 3, 3, 0, 0, 3],   -- base cell 98
    [0, 5, 0, 0, 5, 5, 0],   -- base cell 99
    [0, 0, 1, 3, 1, 0, 1],   -- base cell 100
    [0, 3, 3, 3, 0, 0, 3],   -- base cell 101
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 102
    [0, 0, 1, 3, 1, 0, 1],   -- base cell 103
    [0, 3, 3, 3, 3, 3, 0],   -- base cell 104
    [0, 0, 0, 0, 0, 0, 1],   -- base cell 105
    [0, 0, 1, 0, 3, 5, 1],   -- base cell 106
    [0, -1, 3, 0, 5, 2, 0],  -- base cell 107 (pentagon)
    [0, 5, 0, 0, 5, 5, 0],   -- base cell 108
    [0, 0, 1, 0, 4, 5, 1],   -- base cell 109
    [0, 3, 3, 3, 0, 0, 0],   -- base cell 110
    [0, 0, 0, 3, 0, 5, 0],   -- base cell 111
    [0, 0, 0, 3, 0, 5, 0],   -- base cell 112
    [0, 0, 1, 0, 2, 5, 1],   -- base cell 113
    [0, 0, 0, 0, 0, 0, 1],   -- base cell 114
    [0, 0, 1, 3, 1, 0, 1],   -- base cell 115
    [0, 5, 0, 0, 5, 5, 0],   -- base cell 116
    [0, -1, 1, 0, 3, 4, 2],  -- base cell 117 (pentagon)
    [0, 0, 1, 0, 0, 5, 1],   -- base cell 118
    [0, 0, 0, 0, 0, 0, 1],   -- base cell 119
    [0, 5, 0, 0, 5, 5, 0],   -- base cell 120
    [0, 0, 1, 0, 1, 5, 1]    -- base cell 121
];
begin
 return arr_baseCellNeighbor60CCWRots[basecell +1][neighbor +1]; 
end;
$body$
language plpgsql immutable;

create or replace function NEW_DIGIT_II( currdigit integer, dir integer  )
 returns integer as
$body$
declare
arr_NEW_DIGIT_II integer[7][7] = array
[
    [0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/],
    [1 /*K_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 1 /*K_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 5 /*IK_AXES_DIGIT*/],
    [3 /*JK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/],
    [4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 1 /*K_AXES_DIGIT*/],
    [5 /*IK_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/],
    [6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 1 /*K_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/]
];
begin
 return arr_NEW_DIGIT_II[currdigit +1][dir +1]; 
end;
$body$
language plpgsql immutable;

create or replace function NEW_ADJUSTMENT_II( currdigit integer, dir integer  )
 returns integer as
$body$
declare
arr_NEW_ADJUSTMENT_II integer[7][7] = array
[
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 2 /*J_AXES_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 4 /*I_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 2 /*J_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 6 /*IJ_AXES_DIGIT*/]
];
begin
 return arr_NEW_ADJUSTMENT_II[currdigit +1][dir +1]; 
end;
$body$
language plpgsql immutable;

create or replace function NEW_DIGIT_III( currdigit integer, dir integer  )
 returns integer as
$body$
declare
arr_NEW_DIGIT_III integer[7][7] = array
[
    [0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/],
    [1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/],
    [3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/],
    [4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/],
    [5 /*IK_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/],
    [6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 1 /*K_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/]
];
begin
 return arr_NEW_DIGIT_III[currdigit +1][dir +1]; 
end;
$body$
language plpgsql immutable;

create or replace function NEW_ADJUSTMENT_III( currdigit integer, dir integer  )
 returns integer as
$body$
declare
arr_NEW_ADJUSTMENT_III integer[7][7] = array
[
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/,1 /*K_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/,1 /*K_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 2 /*J_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 6 /*IJ_AXES_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/,4 /*I_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/,4 /*I_AXES_DIGIT*/],
    [0 /*CENTER_DIGIT*/,1 /*K_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/],
    [0 /*CENTER_DIGIT*/, 0 /*CENTER_DIGIT*/, 6 /*IJ_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/,4 /*I_AXES_DIGIT*/, 0 /*CENTER_DIGIT*/, 6 /*IJ_AXES_DIGIT*/]
];
begin
 return arr_NEW_ADJUSTMENT_III[currdigit +1][dir +1]; 
end;
$body$
language plpgsql immutable;

create or replace function DIRECTIONS( dir integer  )
 returns integer as
$body$
declare
arr_DIRECTIONS integer[6] = array 
[ 2 /*J_AXES_DIGIT*/, 3 /*JK_AXES_DIGIT*/,
  1 /*K_AXES_DIGIT*/, 5 /*IK_AXES_DIGIT*/,
  4 /*I_AXES_DIGIT*/, 6 /*IJ_AXES_DIGIT*/
];
begin
 return arr_DIRECTIONS[dir +1]; 
end;
$body$
language plpgsql immutable;


create or replace function _geoToVec3d(lat double precision, lon double precision )
returns vec3d_t as
$body$
declare
 clat double precision := cos( lat );
begin
 return (cos(lon) * clat, sin(lon) * clat, sin(lat) )::vec3d_t;
end;
$body$
language plpgsql immutable;

create or replace function _pointSquareDist(v1 vec3d_t, v2 vec3d_t ) 
returns double precision as  
$body$
declare
 dx double precision := (v1.x - v2.x);
 dy double precision := (v1.y - v2.y);
 dz double precision := (v1.z - v2.z);
begin
 return ((dx * dx) + (dy * dy) + ( dz * dz ));
end;
$body$ 
language plpgsql immutable;

create or replace function _posAngleRads( rads double precision ) 
returns double precision as  
$body$
declare
 M_2PI double precision := 6.28318530717958647692528676655900576839433;
 tmp double precision := rads;
begin

 if rads < 0.0 then
  tmp = rads + M_2PI;
 end if;

 if rads >= M_2PI then
  tmp = tmp - M_2PI;
 end if; 
    
 return tmp;

end;
$body$ 
language plpgsql immutable;

create or replace function _geoAzimuthRads( lat1 double precision, lon1 double precision, lat2 double precision, lon2 double precision ) 
returns double precision as
 $body$  
  select atan2(cos(lat2) * sin(lon2 - lon1),
               cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lon2 - lon1))
 $body$               
language sql immutable;

create or replace function constrainLng( lng double precision ) 
returns double precision as
$body$
declare
 M_PI CONSTANT double precision := 3.14159265358979323846;
begin

 while (lng > M_PI) loop
     lng = lng - (2 * M_PI);
 end loop;

 while (lng < -M_PI) loop
     lng = lng + (2 * M_PI);
 end loop;

 return lng;

end;
$body$ 
language plpgsql immutable;

create or replace function  _geoAzDistanceRads( in_lat double precision, in_lon double precision, az double precision, distance double precision,
                                                out lat double precision, out lon double precision)
as
$body$ 
declare
 EPSILON CONSTANT double precision := 0.0000000000000001;
 M_PI CONSTANT double precision := 3.14159265358979323846;
 M_PI_2 CONSTANT double precision := 1.5707963267948966;
 sinlat double precision;
 sinlon double precision;
 coslon double precision;
begin

 if (distance < EPSILON) then
  lat = in_lat;
  lon = in_lon
  return;
 end if;

 az = _posAngleRads(az);

 if (az < EPSILON or abs(az - M_PI) < EPSILON) then

     if (az < EPSILON) then
         lat = in_lat + distance;
     else  
         lat = in_lat - distance;
     end if;

     if (abs( lat - M_PI_2) < EPSILON)  then
         lat = M_PI_2;
         lon = 0.0;
     elseif ( abs(lat + M_PI_2) < EPSILON) then
         lat = -M_PI_2;
         lon = 0.0;
     else
         lon = constrainLng(in_lon);
     end if;    

 else  
 
     sinlat = sin(in_lat) * cos(distance) + cos(in_lat) * sin(distance) * cos(az);
     if (sinlat > 1.0) then 
      sinlat = 1.0;
     end if;

     if (sinlat < -1.0) then 
      sinlat = -1.0;
     end if;

     lat = asin(sinlat);

     if ( abs( lat - M_PI_2) < EPSILON) then 
         lat = M_PI_2;
         lon = 0.0;
     elseif (abs(lat + M_PI_2) < EPSILON) then 
         lat = -M_PI_2;
         lon = 0.0;
     else 
         sinlon = sin(az) * sin(distance) / cos(lat);
         coslon = (cos(distance) - sin(in_lat) * sin(lat)) / cos(in_lat) / cos(lat);
         if (sinlon >  1.0) then sinlon =  1.0; end if;
         if (sinlon < -1.0) then sinlon = -1.0; end if;
         if (coslon >  1.0) then sinlon =  1.0; end if;
         if (coslon < -1.0) then sinlon = -1.0; end if;
         lon = constrainLng(in_lon + atan2(sinlon, coslon));
     end if;

 end if;

end;
$body$ 
language plpgsql immutable;

create or replace function isResClassIII( res integer ) 
returns boolean as
$body$
 select (res % 2) = 1
$body$ 
language sql immutable;

create or replace function _v2dMag( x double precision, y double precision ) 
returns double precision as
$body$  
 select sqrt( x * x + y * y )
$body$ 
language sql immutable;

create or replace function _v2dIntersect( p0_x double precision, p0_y double precision,
                                          p1_x double precision, p1_y double precision,
                                          p2_x double precision, p2_y double precision,
                                          p3_x double precision, p3_y double precision,
                                          out x double precision, out y double precision 
                                        )
as  
$body$
declare
 s1_x double precision := p1_x - p0_x;
 s1_y double precision := p1_y - p0_y;
 s2_x double precision := p3_x - p2_x;
 s2_y double precision := p3_y - p2_y;
 t double precision := 0.0;
begin 

   t = (s2_x * (p0_y - p2_y) - s2_y * (p0_x - p2_x)) / (-s2_x * s1_y + s1_x * s2_y);

   x = p0_x + (t * s1_x);
   y = p0_y + (t * s1_y);

end;
$body$
language plpgsql immutable;

create or replace function _v2dEquals( v1_x double precision, v1_y double precision,
                                       v2_x double precision, v2_y double precision
                                     )
returns boolean as  
$body$
 select ( (v1_x = v2_x) and (v1_y = v2_y) )
$body$ 
language sql immutable;


create or replace function _hex2dToGeo( x double precision, y double precision, face integer, res integer, substrate boolean,
                                        out lat double precision, out lon double precision )
as       
$body$ 
declare
 EPSILON CONSTANT double precision := 0.0000000000000001;
 M_AP7_ROT_RADS CONSTANT double precision := 0.333473172251832115336090755351601070065900389;
 M_SQRT7 CONSTANT double precision := 2.6457513110645905905016157536392604257102;
 RES0_U_GNOMONIC CONSTANT double precision := 0.38196601125010500003;

 r double precision := 0.0;
 theta double precision := 0.0;
 center record;
 g record;
begin

 r = _v2dMag(x,y);
 center = (_faceCenterGeo(face));

 if r < EPSILON then
  lat = center.lat;
  lon = center.lon;
  return;
 end if;

 theta = atan2( y, x );

 r = r / power( M_SQRT7, res );

 if substrate then
  r = r / 3.0;
  if (isResClassIII(res)) then 
   r = r / M_SQRT7;
  end if;  
 end if;

 r = atan( r * RES0_U_GNOMONIC );

 if (not substrate) and isResClassIII(res) then
  theta = _posAngleRads(theta + M_AP7_ROT_RADS);
 end if;

 theta = _posAngleRads( (_faceAxesAzRadsCII(face)).v0 - theta );

 g = _geoAzDistanceRads(center.lat, center.lon, theta, r);

 lat = g.lat;
 lon = g.lon;

end;
$body$ 
language plpgsql immutable;

create or replace function _geoToHex2d( lat double precision, lon double precision, res integer, 
                                        out face integer, out x double precision, out y double precision  )
as       
$body$ 
declare
 NUM_ICOSA_FACES CONSTANT integer := 20;
 EPSILON CONSTANT double precision := 0.0000000000000001;
 M_AP7_ROT_RADS CONSTANT double precision := 0.333473172251832115336090755351601070065900389;
 RES0_U_GNOMONIC CONSTANT double precision := 0.38196601125010500003;
 M_SQRT7 CONSTANT double precision := 2.6457513110645905905016157536392604257102;

 v3d vec3d_t; 
 face3d vec3d_t;
 sqd double precision := 9999999.0;
 sqdT double precision := 0.0;
 r double precision := 0.0;
 theta double precision := 0.0;
 center record;
begin
 v3d = _geoToVec3d( lat, lon );
 face = 0;

 for i in 0..(NUM_ICOSA_FACES -1) loop
  face3d = _faceCenterPoint( i );
  sqdT = _pointSquareDist( face3d, v3d );
  if sqdT < sqd then
   face = i;
   sqd = sqdT;
  end if;
 end loop;

 r = acos(1 - sqd / 2);
 if r < EPSILON then
  x = 0.0;
  y = 0.0;
  return;
 end if;

 center = (_faceCenterGeo(face));

 theta = _posAngleRads( (_faceAxesAzRadsCII(face)).v0 - _posAngleRads( _geoAzimuthRads( center.lat, center.lon, lat, lon ) ) );

 if isResClassIII(res) then
  theta = _posAngleRads(theta - M_AP7_ROT_RADS);
 end if; 

 r = (tan(r) / RES0_U_GNOMONIC) * power( M_SQRT7, res );

 x = r * cos(theta);
 y = r * sin(theta);

end;
$body$ 
language plpgsql immutable;

create or replace function _ijkToHex2d( i integer, j integer, k integer,
                                        out x double precision, out y double precision)
as
$body$ 
declare
 M_SQRT3_2 CONSTANT double precision := 0.8660254037844386467637231707529361834714;
 i1 integer;
 j1 integer;
begin
 
 i1 = i - k;
 j1 = j - k;
 x = i1 - 0.5 * j1;
 y = j1 * M_SQRT3_2;

end;
$body$ 
language plpgsql immutable;

create or replace function _ijkMatches( i1 integer, j1 integer, k1 integer, i2 integer, j2 integer, k2 integer )  
 returns boolean as
 $body$ 
  select ( i1 = i2 and j1 = j2 and k1 = k2 )
 $body$  
language sql immutable;

create or replace function _ijkAdd( i1 integer, j1 integer, k1 integer, i2 integer, j2 integer, k2 integer,
                                    out i integer, out j integer, out k integer )  
as
$body$
declare
begin

  i = i1 + i2;
  j = j1 + j2;
  k = k1 + k2;

end;
$body$ 
language plpgsql immutable;

create or replace function _ijkSub( i1 integer, j1 integer, k1 integer, i2 integer, j2 integer, k2 integer,
                                    out i integer, out j integer, out k integer )  
as
$body$
declare
begin

  i = i1 - i2;
  j = j1 - j2;
  k = k1 - k2;

end;
$body$ 
language plpgsql immutable;


create or replace function _ijkScale( inout i integer, inout j integer, inout k integer, factor integer)  
as
$body$
declare
begin

  i = i * factor;
  j = j * factor;
  k = k * factor;

end;
$body$ 
language plpgsql immutable;

create or replace function _ijkId( inout i integer, inout j integer, inout k integer )  
as
 $body$
  select i,j,k
 $body$
language sql immutable;


create or replace function _ijkNormalize( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 min integer;
begin

if (i < 0) then
    j = j - i;
    k = k - i;
    i = 0;
end if;

if (j < 0) then
    i = i - j;
    k = k - j;
    j = 0;
end if;

if (k < 0) then 
    i = i - k;
    j = j - k;
    k = 0;
end if;

min = i;
if (j < min) then 
 min = j;
end if;

if (k < min) then 
 min = k;
end if; 

if (min > 0) then
    i = i - min;
    j = j - min;
    k = k - min;
end if;

end;
$body$ 
language plpgsql immutable;


create or replace function _upAp7( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 i1 integer := i - k;
 j1 integer := j - k;
 r record;
begin

  i = round((3 * i1 - j1) / 7.0);
  j = round((i1 + 2 * j1) / 7.0);
  k = 0;
  
  r =  _ijkNormalize( i, j, k );

  i = r.i; 
  j = r.j; 
  k = r.k;

end;
$body$ 
language plpgsql immutable;

create or replace function _upAp7r( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 i1 integer := i - k;
 j1 integer := j - k;
 r record;
begin

  i = round((2 * i1 + j1) / 7.0);
  j = round((3 * j1 - i1) / 7.0);
  k = 0;
  
  r =  _ijkNormalize( i, j, k );

  i = r.i; 
  j = r.j; 
  k = r.k;

end;
$body$ 
language plpgsql immutable;

create or replace function _downAp7( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 3,0,1, i);
 jvec = _ijkScale( 1,3,0, j);
 kvec = _ijkScale( 0,1,3, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;

create or replace function _downAp7r( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 3,1,0, i);
 jvec = _ijkScale( 0,3,1, j);
 kvec = _ijkScale( 1,0,3, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;

create or replace function _downAp3( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 2,0,1, i);
 jvec = _ijkScale( 1,2,0, j);
 kvec = _ijkScale( 0,1,2, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;

create or replace function _downAp3r( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 2,1,0, i);
 jvec = _ijkScale( 0,2,1, j);
 kvec = _ijkScale( 1,0,2, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;


create or replace function _ijkRotate60ccw( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 1, 1, 0, i);
 jvec = _ijkScale( 0, 1, 1, j);
 kvec = _ijkScale( 1, 0, 1, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;


create or replace function _ijkRotate60cw( inout i integer, inout j integer, inout k integer )  
as
$body$
declare
 ivec record;
 jvec record;
 kvec record;
 r record;
begin

 ivec = _ijkScale( 1, 0, 1, i);
 jvec = _ijkScale( 1, 1, 0, j);
 kvec = _ijkScale( 0, 1, 1, k);

 r =  _ijkNormalize( ivec.i + jvec.i + kvec.i, ivec.j + jvec.j + kvec.j, ivec.k + jvec.k + kvec.k );

 i = r.i; 
 j = r.j; 
 k = r.k;

end;
$body$ 
language plpgsql immutable;


create or replace function UNIT_VECS( direction integer, out i integer, out j integer, out k integer )
as
$body$
declare
 _UNIT_VECS integer [][3] = array
 [
  [0, 0, 0],  -- direction 0
  [0, 0, 1],  -- direction 1
  [0, 1, 0],  -- direction 2
  [0, 1, 1],  -- direction 3
  [1, 0, 0],  -- direction 4
  [1, 0, 1],  -- direction 5
  [1, 1, 0]   -- direction 6
 ];
begin
 direction = direction + 1;
 i = _UNIT_VECS[direction][1];
 j = _UNIT_VECS[direction][2];
 k = _UNIT_VECS[direction][3];
end;
$body$ 
language plpgsql immutable;

create or replace function _unitIjkToDigit( i integer, j integer, k integer )
returns integer as
$body$
declare
 INVALID_DIGIT CONSTANT integer := 7;
 NUM_DIGITS CONSTANT integer := 7;
 CENTER_DIGIT integer := 0;
 c record;
 e record;
begin
 c = _ijkNormalize( i,j,k );
 for i in CENTER_DIGIT..(NUM_DIGITS - 1) loop
  e = UNIT_VECS( i );
  if _ijkMatches(c.i,c.j,c.k,e.i,e.j,e.k) then
   return i;
  end if;
 end loop;

 return INVALID_DIGIT;
end;
$body$ 
language plpgsql immutable;

create or replace function _neighbor( inout i integer, inout j integer, inout k integer, direction integer)  
as
$body$
declare
 NUM_DIGITS CONSTANT integer := 7;
 CENTER_DIGIT integer := 0;
 r record;
 e record;
begin

 if direction > CENTER_DIGIT and direction < NUM_DIGITS then

  e = UNIT_VECS( direction ); 
  r = _ijkAdd(i,j,k,e.i,e.j,e.k);
  r = _ijkNormalize(r.i,r.j,r.k);
  i = r.i;
  j = r.j;
  k = r.k;

 end if;
end;
$body$ 
language plpgsql immutable;

create or replace function _hex2dToCoordIJK( x double precision, y double precision,
                                             out i integer, out j integer, out k integer )
as
$body$
declare
 a1 double precision;
 a2 double precision;
 x1 double precision;
 x2 double precision;
 m1 integer;
 m2 integer;
 r1 double precision;
 r2 double precision;
 M_SIN60 CONSTANT double precision := 0.8660254037844386467637231707529361834714;
 r record;
begin

 k = 0;

 a1 = abs(x);
 a2 = abs(y);
 
 -- RAISE NOTICE 'a1(%) a2(%)', a1, a2;

 x2 = a2 / M_SIN60;
 x1 = a1 + x2 / 2.0;
 
 -- RAISE NOTICE 'x1(%) x2(%)', x1, x2;

 m1 = trunc(x1);
 m2 = trunc(x2);
 
 -- RAISE NOTICE 'm1(%) m2(%)', m1, m2;

 r1 = x1 - m1;
 r2 = x2 - m2;
 
 -- RAISE NOTICE 'r1(%) r2(%)', r1, r2;

 if (r1 < 0.5) then
     if (r1 < 1.0 / 3.0) then
         if (r2 < (1.0 + r1) / 2.0) then
             i = m1;
             j = m2;
         else 
             i = m1;
             j = m2 + 1;
         end if;
     else 
         if (r2 < (1.0 - r1)) then
             j = m2;
         else 
             j = m2 + 1;
         end if;
         if (((1.0 - r1) <= r2) and (r2 < (2.0 * r1))) then
             i = m1 + 1;
         else 
             i = m1;
         end if;
     end if;
 else 
     if (r1 < 2.0 / 3.0) then
         if (r2 < (1.0 - r1))  then
             j = m2;
         else
             j = m2 + 1;
         end if;
         if (((2.0 * r1 - 1.0) < r2) and ( r2 < (1.0 - r1))) then
             i = m1;
         else
             i = m1 + 1;
         end if;
     else 
         if (r2 < (r1 / 2.0)) then
             i = m1 + 1;
             j = m2;
         else
             i = m1 + 1;
             j = m2 + 1;
         end if;
     end if;
 end if;
 
 if ( x < 0.0 ) then
     if ((j % 2) = 0)  then
         i = i - (2.0 * (i - (j / 2)));
     else 
         i = i - (2.0 * (i - ((j + 1) / 2)) + 1);
     end if;
 end if;
 if ( y < 0.0 ) then
     i = i - (2 * j + 1) / 2;
     j = -1 * j;
 end if;
 
 -- RAISE NOTICE 'i(%) j(%) k(%)', i, j, k;

 r = _ijkNormalize(i,j,k);
 i = r.i;
 j = r.j;
 k = r.k;
 
end;
$body$ 
language plpgsql immutable;

create or replace function _geoToFaceIjk(lat double precision, lon double precision, res integer)
 returns face_ijk_t as
$body$ 
declare
 h2d record;
 coord record;
begin
 
 h2d = _geoToHex2d( lat, lon, res ); -- fills  r.face, r.x, r.y
 
 coord = _hex2dToCoordIJK(h2d.x, h2d.y);

  return (h2d.face,coord.i,coord.j,coord.k);
end;
$body$ 
language plpgsql immutable;

create or replace function _geoToFaceIjkDeg(lat double precision, lon double precision, res integer)
 returns face_ijk_t as
$body$ 
 select _geoToFaceIjk( radians(lat), radians(lon), res )
$body$ 
language sql immutable;



create or replace function H3_GET_MODE( h H3Index ) 
returns integer as 
-- H3_MODE_OFFSET = 59, H3_MODE_MASK = 8646911284551352320
 $body$
  select ( (h & (8646911284551352320)) >> 59 )::integer
 $body$
language sql immutable;

create or replace function H3_SET_MODE( h H3Index, mode integer ) 
returns H3Index as 
-- H3_MODE_OFFSET = 59, H3_MODE_MASK = 8646911284551352320
 $body$
  select (( h & (~8646911284551352320)) | ( ( mode::bigint & 15) << 59 ))::H3Index
 $body$
language sql immutable;

create or replace function H3_GET_RESOLUTION( h H3Index ) 
returns integer as 
-- H3_RES_OFFSET = 52, H3_RES_MASK = 67553994410557440
 $body$
  select ( (h & (67553994410557440)) >> 52 )::integer
 $body$
language sql immutable;

create or replace function H3_SET_RESOLUTION( h H3Index, res integer ) 
returns H3Index as 
-- H3_RES_OFFSET = 52, H3_RES_MASK = 67553994410557440
 $body$
  select (( h & (~67553994410557440)) | ( ( res::bigint & 15) << 52 ))::H3Index
 $body$ 
language sql immutable;

create or replace function H3_GET_BASE_CELL( h H3Index ) 
returns integer as 
-- H3_BC_OFFSET = 45, H3_BC_MASK = 4468415255281664
 $body$
  select ( (h & (4468415255281664)) >> 45 )::integer
 $body$
language sql immutable;

create or replace function H3_SET_BASE_CELL( h H3Index, bc integer ) 
returns H3Index as 
-- H3_BC_OFFSET = 45, H3_BC_MASK = 4468415255281664
 $body$
  select (( h & (~4468415255281664)) | ( ( bc::bigint & 127) << 45 ))::H3Index
 $body$
language sql immutable;

create or replace function H3_GET_INDEX_DIGIT( h H3Index, res integer ) 
returns integer as 
-- H3_PER_DIGIT_OFFSET = 3, H3_DIGIT_MASK = 7, MAX_H3_RES = 15 , res = 1,..,15
 $body$
  select (  ( h >> ( (15 - res) * 3 ) ) & 7 )::integer
 $body$
language sql immutable;

create or replace function H3_SET_INDEX_DIGIT( h H3Index, res integer, digit integer ) 
returns H3Index as 
-- H3_PER_DIGIT_OFFSET = 3, H3_DIGIT_MASK = 7, MAX_H3_RES = 15 , res = 1,..,15
 $body$
  select (( h & (~ (7::bigint << ((15 - res) * 3)))) | ( (digit::bigint & 7) << ((15 - res) * 3)))::H3Index
 $body$
language sql immutable;

create or replace function h3GetResolution( h H3Index ) 
returns integer as
 $body$
  select H3_GET_RESOLUTION(h)
 $body$
language sql immutable;

create or replace function h3GetBaseCell( h H3Index)
returns integer as
 $body$
  select H3_GET_BASE_CELL(h)
 $body$
language sql immutable; 

create or replace function h3IsResClassIII( h H3Index)
returns boolean as
 $body$
  select ( H3_GET_RESOLUTION(h) % 2 ) != 0
 $body$
language sql immutable; 

create or replace function h3IsParent( parent H3Index, child H3Index )
returns boolean as -- H3_RES_MASK = 67553994410557440
 $body$
  select ( (( parent & (~67553994410557440)) >> ((15 - H3_GET_RESOLUTION(parent)) * 3 ) ) = 
           (( child &  (~67553994410557440)) >> ((15 - H3_GET_RESOLUTION(parent)) * 3 ) ) )
 $body$          
language sql immutable; 

create or replace function h3IsValid( h H3Index )
returns boolean as
$body$
declare
 H3_HEXAGON_MODE CONSTANT integer := 1;
 NUM_BASE_CELLS CONSTANT integer := 122;
 NUM_DIGITS CONSTANT integer := 7;
 CENTER_DIGIT CONSTANT integer := 0;
 INVALID_DIGIT CONSTANT integer := 7;
 MAX_H3_RES CONSTANT integer := 15;
 baseCell integer;
 res integer;
 digit integer;
begin

 if (H3_GET_MODE(h) != H3_HEXAGON_MODE) then 
  return false;
 end if;

 baseCell = H3_GET_BASE_CELL(h);
 if ( (baseCell < 0) or (baseCell >= NUM_BASE_CELLS)) then
  return false;
 end if;

 res = H3_GET_RESOLUTION(h);
 if ( (res < 0) or (res > MAX_H3_RES)) then 
  return false;
 end if;

 for r in 1..res loop
  digit = H3_GET_INDEX_DIGIT(h, r);
  if ( (digit < CENTER_DIGIT) or (digit >= NUM_DIGITS)) then
   return false;
  end if;
 end loop; 

 for  r in (res + 1)..MAX_H3_RES loop
  digit = H3_GET_INDEX_DIGIT(h, r);
  if (digit != INVALID_DIGIT) then 
   return false;
  end if; 
 end loop;

 return true;
end;
$body$ 
language plpgsql immutable;

create or replace function _rotate60ccw(direction integer)
returns integer as
 $body$
  select 
   case direction 
    when 1 then 5
    when 5 then 4
    when 4 then 6
    when 6 then 2
    when 2 then 3
    when 3 then 1
    else direction
   end
 $body$   
language sql immutable;

create or replace function _rotate60cw(direction integer)
returns integer as
$body$
 select 
   case direction 
    when 1 then 3
    when 3 then 2
    when 2 then 6
    when 6 then 4
    when 4 then 5
    when 5 then 1
    else direction
   end
$body$   
language sql immutable;

create or replace function _h3LeadingNonZeroDigit(h H3Index) 
returns integer as 
$body$ 
declare
 digit integer;
 CENTER_DIGIT CONSTANT integer := 0;
begin 
 
 for r in 1..H3_GET_RESOLUTION(h) loop

  digit = H3_GET_INDEX_DIGIT(h, r);

  if digit != CENTER_DIGIT then 
   return digit;
  end if;

 end loop;

 return CENTER_DIGIT;

end;
$body$ 
language plpgsql immutable;


create or replace function _h3Rotate60cw( h H3Index) 
returns H3Index as
$body$
declare
begin

 for r in 1..H3_GET_RESOLUTION(h) loop
  h = H3_SET_INDEX_DIGIT(h, r, _rotate60cw( H3_GET_INDEX_DIGIT(h, r) ) );
 end loop;

 return h;

end;
$body$
language plpgsql immutable;

create or replace function _h3Rotate60ccw( h H3Index) 
returns H3Index as
$body$
declare
begin

 for r in 1..H3_GET_RESOLUTION(h) loop
  h = H3_SET_INDEX_DIGIT(h, r, _rotate60ccw( H3_GET_INDEX_DIGIT(h, r) ) );
 end loop;

 return h;

end;
$body$
language plpgsql immutable;

create or replace function _h3RotatePent60ccw( h H3Index) 
returns H3Index as
$body$
declare
 K_AXES_DIGIT CONSTANT integer := 1;
 foundFirstNonZeroDigit integer := 0;
 res integer;
 digit integer;
begin

 res = H3_GET_RESOLUTION(h);

 for r in 1..res loop
  digit = _rotate60ccw( H3_GET_INDEX_DIGIT(h, r));
  h = H3_SET_INDEX_DIGIT(h, r, digit );
  if ( foundFirstNonZeroDigit = 0  and digit != 0) then
   foundFirstNonZeroDigit = 1;
            
   if (_h3LeadingNonZeroDigit(h) = K_AXES_DIGIT) then
    h = _h3Rotate60ccw(h);
   end if;
  end if;
 end loop;

 return h;

end;
$body$
language plpgsql immutable;

create or replace function _h3RotatePent60cw( h H3Index) 
returns H3Index as
$body$
declare
 K_AXES_DIGIT CONSTANT integer := 1;
 foundFirstNonZeroDigit integer := 0;
 res integer;
 digit integer;
begin

 res = H3_GET_RESOLUTION(h);

 for r in 1..res loop
  digit = _rotate60cw( H3_GET_INDEX_DIGIT(h, r));
  h = H3_SET_INDEX_DIGIT(h, r, digit );
  if ( foundFirstNonZeroDigit = 0 and digit != 0) then
   foundFirstNonZeroDigit = 1;
            
   if (_h3LeadingNonZeroDigit(h) = K_AXES_DIGIT) then
    h = _h3Rotate60cw(h);
   end if;
  end if;
 end loop;

 return h;

end;
$body$
language plpgsql immutable;

create or replace function _faceIjkToH3( fijk face_ijk_t, res integer)
 returns h3index as
$body$ 
declare
 MAX_FACE_COORD CONSTANT integer := 2;
 K_AXES_DIGIT CONSTANT integer := 1;
 H3_INVALID_INDEX CONSTANT h3index := 0;
 H3_INIT CONSTANT H3Index := 35184372088831; -- 0000000000000000000111111111111111111111111111111111111111111111
 H3_HEXAGON_MODE CONSTANT integer := 1;
 h H3Index := H3_INIT;
 ijk record;
 lastIJK record;
 lastCenter record;
 diff record;
 baseCell integer;
 rBaseCell record;
 numRots integer;
begin

 h = H3_SET_RESOLUTION( H3_SET_MODE(h, H3_HEXAGON_MODE), res );

 if ( res = 0 ) then
  if ( fijk.i > MAX_FACE_COORD or fijk.j > MAX_FACE_COORD or fijk.k > MAX_FACE_COORD ) then
   return H3_INVALID_INDEX;
  end if;

  return H3_SET_BASE_CELL(h, _faceIjkToBaseCell(fijk.face,fijk.i, fijk.j, fijk.k ));
 end if;

 --   FaceIJK fijkBC = *fijk;

 ijk = _ijkId( fijk.i, fijk.j, fijk.k );
 for r in reverse (res - 1)..0 loop
  lastIJK = ijk;
  if( isResClassIII( r+1 ) ) then
   ijk = _upAp7(ijk.i,ijk.j,ijk.k);
   lastCenter = _downAp7( ijk.i, ijk.j, ijk.k );
  else
   ijk = _upAp7r(ijk.i,ijk.j,ijk.k);
   lastCenter = _downAp7r( ijk.i, ijk.j, ijk.k );
  end if;

  diff = _ijkSub( lastIJK.i, lastIJK.j, lastIJK.k, lastCenter.i, lastCenter.j, lastCenter.k );
  diff = _ijkNormalize(diff.i, diff.j, diff.k );
  h = H3_SET_INDEX_DIGIT(h, r + 1, _unitIjkToDigit(diff.i,diff.j,diff.k));
 end loop;

 if ( ijk.i > MAX_FACE_COORD or ijk.j > MAX_FACE_COORD or ijk.k > MAX_FACE_COORD ) then
  return H3_INVALID_INDEX;
 end if;

 baseCell = _faceIjkToBaseCell( fijk.face, ijk.i, ijk.j, ijk.k );
 h = H3_SET_BASE_CELL(h, baseCell);

 numRots = _faceIjkToBaseCellCCWrot60( fijk.face, ijk.i, ijk.j, ijk.k );

 rBaseCell = baseCellData( baseCell );
 
 if rBaseCell.pentagon then
  
  if _h3LeadingNonZeroDigit(h) = K_AXES_DIGIT then
   if _baseCellIsCwOffset( baseCell, fijk.face ) then
    h = _h3Rotate60cw(h);
   else
    h = _h3Rotate60ccw(h);
   end if;
  end if;

  for i in 0..(numRots-1) loop
   h = _h3RotatePent60ccw(h);
  end loop;
 else
  for i in 0..(numRots-1) loop
   h = _h3Rotate60ccw(h);
  end loop;

    
 end if;

 return h;
end;
$body$ 
language plpgsql immutable;

create or replace function _h3ToFaceIjkWithInitializedFijk( h h3index, inout fijk face_ijk_t, out possible_overage boolean )
as
$body$
declare
 res integer;
 baseCell integer;
 rBaseCell record;
 ijk record;
  
begin
 possible_overage = true;

 baseCell = H3_GET_BASE_CELL(h);
 rBaseCell = baseCellData( baseCell );

 res = H3_GET_RESOLUTION(h);

 if ( (not rBaseCell.pentagon) and ( (res = 0) or ( fijk.i = 0 and fijk.j = 0 and fijk.k = 0  ) ) ) then
  possible_overage = false;
 end if;

 for r in 1..res loop

  --RAISE NOTICE 'a % fijk(%)', r,fijk;

  if ( isResClassIII(r) ) then
   ijk = _downAp7(fijk.i,fijk.j,fijk.k);
   fijk.i = ijk.i; 
   fijk.j = ijk.j;
   fijk.k = ijk.k;
  else
   ijk = _downAp7r(fijk.i,fijk.j,fijk.k);
   fijk.i = ijk.i; 
   fijk.j = ijk.j;
   fijk.k = ijk.k;
  end if;
 
  --RAISE NOTICE 'b % fijk(%) %', r,fijk, H3_GET_INDEX_DIGIT(h, r);

  ijk = _neighbor(fijk.i,fijk.j,fijk.k, H3_GET_INDEX_DIGIT(h, r));

  fijk.i = ijk.i; 
  fijk.j = ijk.j;
  fijk.k = ijk.k;

 end loop;

end;
$body$ 
language plpgsql immutable;

create or replace function _adjustOverageClassII( inout fijk face_ijk_t, res integer, pentleading4 boolean, substrate boolean, out overage integer )
as
$body$
declare
 CENTER CONSTANT integer := 0; /** Center faceNeighbors table direction */
 IJ CONSTANT integer := 1; /** IJ quadrant faceNeighbors table direction */
 KI CONSTANT integer := 2; /** KI quadrant faceNeighbors table direction */
 JK CONSTANT integer := 3; /** JK quadrant faceNeighbors table direction */

 maxDim integer;
 unitScale integer;
 fijkOrient record;
 tmpIJK record;

begin

 overage = 0;
 maxDim = maxDimByCIIres(res);
 if( substrate ) then
  maxDim = 3 * maxDim;
 end if;

 
 if ( substrate and ( fijk.i + fijk.j + fijk.k ) = maxDim ) then
  overage = 1;
 elseif ( ( fijk.i + fijk.j + fijk.k ) > maxDim ) then

  overage = 2;

  if (fijk.k > 0 ) then
   if ( fijk.j > 0 ) then -- jk "quadrant"
    fijkOrient = faceNeighbors(fijk.face,JK);
   else -- ik "quadrant"
    fijkOrient = faceNeighbors(fijk.face,KI);
    if (pentLeading4) then
      tmpIJK = _ijkSub(fijk.i,fijk.j,fijk.k,maxDim,0,0);
      tmpIJK = _ijkRotate60cw(tmpIJK.i,tmpIJK.j,tmpIJK.k);
      tmpIJK = _ijkAdd(tmpIJK.i,tmpIJK.j,tmpIJK.k,maxDim,0,0);
      fijk.i = tmpIJK.i;
      fijk.j = tmpIJK.j;
      fijk.k = tmpIJK.k;
    end if;
   end if;

  else -- ij "quadrant"
   fijkOrient = faceNeighbors(fijk.face,IJ);
  end if;

  fijk.face = fijkOrient.face;
  
  for i in 0..(fijkOrient.ccwrot60 - 1) loop 
   tmpIJK = _ijkRotate60ccw(fijk.i,fijk.j,fijk.k);
   fijk.i = tmpIJK.i;
   fijk.j = tmpIJK.j;
   fijk.k = tmpIJK.k;
  end loop;
 
  unitScale = unitScaleByCIIres(res);

  if (substrate) then 
   unitScale = 3* unitScale;
  end if;

  tmpIJK = _ijkScale( fijkOrient.i, fijkOrient.j, fijkOrient.k, unitScale );
  tmpIJK = _ijkNormalize( fijk.i + tmpIJK.i, fijk.j + tmpIJK.j, fijk.k + tmpIJK.k );
  fijk.i = tmpIJK.i;
  fijk.j = tmpIJK.j;
  fijk.k = tmpIJK.k;

  if ( substrate and ( fijk.i + fijk.j + fijk.k ) = maxDim ) then
   overage = 1;
  end if;

 end if;

end;
$body$
language plpgsql immutable;

create or replace function _h3ToFaceIjk( h h3index, out fijk face_ijk_t ) as
$body$ 
declare
 
 baseCell integer;
 rBaseCell record;
 ijk record;
 r1 record;
 r1a record;
 r2 record;
 r2a record;
 res integer;
 pentLeading4 boolean;

begin

 baseCell = H3_GET_BASE_CELL(h);
 rBaseCell = baseCellData( baseCell );

 if ( rBaseCell.pentagon and _h3LeadingNonZeroDigit(h) = 5) then
  h = _h3Rotate60cw(h);
 end if;

 fijk.face = rBaseCell.face;
 fijk.i = rBaseCell.i;
 fijk.j = rBaseCell.j;
 fijk.k = rBaseCell.k;

 r1 = _h3ToFaceIjkWithInitializedFijk( h, fijk );

 --RAISE NOTICE 'r1(%)', r1;
 --RAISE NOTICE 'fijk(%)', fijk;

 r1a = r1.fijk;
 fijk.i = r1a.i;
 fijk.j = r1a.j;
 fijk.k = r1a.k; -- r1.fijk kept for backup


 if ( not r1.possible_overage ) then
  return;
 end if;

 
 res = H3_GET_RESOLUTION(h);

 if ( isResClassIII(res) ) then
  ijk = _downAp7r(fijk.i,fijk.j,fijk.k);
  fijk.i = ijk.i; 
  fijk.j = ijk.j;
  fijk.k = ijk.k;
  res = res + 1;
 end if;

 pentLeading4 = ( rBaseCell.pentagon and _h3LeadingNonZeroDigit(h) = 4 );

 r2 = _adjustOverageClassII( fijk, res, pentLeading4, false );
 --RAISE NOTICE 'r2(%)', r2;
 --RAISE NOTICE 'fijk(%) res(%) p(%)', fijk, res,pentLeading4;
 r2a = r2.fijk;
 fijk.face = r2a.face; 
 fijk.i    = r2a.i; 
 fijk.j    = r2a.j;
 fijk.k    = r2a.k;

 if ( r2.overage > 0 ) then
 
  if ( rBaseCell.pentagon ) then
   -- for abc in 1..3 /*dbg*/
   loop
     
     r2 = _adjustOverageClassII( fijk, res, false, false );
     -- RAISE NOTICE '%, fijk(%) res(%) r2(%)', abc, fijk, res, r2;
     r2a = r2.fijk;
     fijk.face = r2a.face; 
     fijk.i    = r2a.i; 
     fijk.j    = r2a.j;
     fijk.k    = r2a.k;
    
     if ( r2.overage = 0 ) then
      exit;
     end if;
   end loop;
  end if;
  if (res != H3_GET_RESOLUTION(h)) then
    r2 = _upAp7r( fijk.i, fijk.j, fijk.k );
    fijk.i    = r2.i; 
    fijk.j    = r2.j;
    fijk.k    = r2.k;
  end if;
 elseif ( res != H3_GET_RESOLUTION(h) ) then

  fijk.i = r1a.i;
  fijk.j = r1a.j;
  fijk.k = r1a.k; -- r1.fijk kept for backup

 end if;
 
end;
$body$ 
language plpgsql immutable;


create or replace function geoToH3( lat double precision, lon double precision, res integer)
 returns h3index as
$body$
 select case  
         when ( (res < 0) or ( res >  15 /*MAX_H3_RES*/) ) then 0::h3index /* H3_INVALID_INDEX CONSTANT */
         else _faceIjkToH3( _geoToFaceIjk(lat, lon, res) , res)
        end  
$body$
language sql immutable;

create or replace function geoToH3Deg_p( c geometry, res integer)
 returns h3index as
$body$
 select geoToH3(radians(st_y(c)), radians(st_x(c)), res)
$body$
language sql immutable;

create table if not exists h3cache ( h3 h3index not null, res integer not null,  geo geometry not null );
create index if not exists idx_h3cache_geo on h3cache using gist (geo);
create index if not exists idx_h3cache_res on h3cache ( res );

alter table h3cache drop constraint if exists pk_h3;
create index if not exists idx_h3cache_h3 on h3cache ( h3 );


create or replace function geoToH3Deg( c geometry, res integer)
 returns h3index as
$body$
declare
 h H3Index;
 hg geometry;
begin

 select t.h3 into h from h3cache t where t.geo && c and st_intersects(t.geo,c) and t.res = geoToH3Deg.res limit 1;

 if( h notnull ) then
  return h;
 end if;

 h = geoToH3Deg_p(c,res);
 hg = h3ToGeoBoundaryDeg_p( h );
 
 insert into h3cache ( h3, res, geo ) values( h,res, hg );

 return h; 
 
end;
$body$
language plpgsql volatile;


create or replace function h3IsPentagon( h H3Index ) 
returns boolean as 
$body$
declare
 baseCell integer;
 rBaseCell record;
begin

 baseCell = H3_GET_BASE_CELL(h);
 rBaseCell = baseCellData( baseCell );
 return ( rBaseCell.pentagon and _h3LeadingNonZeroDigit(h) = 0);
end;
$body$
language plpgsql immutable;

create or replace function _faceIjkPentToGeoBoundary( h face_ijk_t, res integer )
returns geometry as
$body$
declare
    M_SQRT3_2 CONSTANT double precision := 0.8660254037844386467637231707529361834714;
    NUM_PENT_VERTS CONSTANT integer := 5;
    verts integer [5][3];
    adjRes integer;
    centerIJK face_ijk_t;
    r record;
    lastFijk face_ijk_t;
    fijkVerts face_ijk_t[5];
    cp face_ijk_t;
    fijk face_ijk_t;
    tmpFijk face_ijk_t;
    g_numVers integer := 0;
    v integer;
    pentLeading4 boolean;
    overage integer;
    r1 record;
    r1a record;
    orig2d0 record;
    orig2d1 record;
    currentToLastDir integer;
    fijkOrient record;
    tmpIJK record;
    transVec record;
    maxDim integer;
    edge0 double precision[2];
    edge1 double precision[2];
    inter record;
    arr_pts geometry[];
    pnt record;
    vec record;
begin

--  RAISE NOTICE 'IN-->, h(%) res(%)', h, res;

 if (isResClassIII(res)) then
   verts = array 
    [
        [5, 4, 0],  -- 0
        [1, 5, 0],  -- 1
        [0, 5, 4],  -- 2
        [0, 1, 5],  -- 3
        [4, 0, 5]   -- 4
    ];
 else
   verts = array
    [
        [2, 1, 0],  -- 0
        [1, 2, 0],  -- 1
        [0, 2, 1],  -- 2
        [0, 1, 2],  -- 3
        [1, 0, 2]   -- 4
    ];
 end if;

 centerIJK = h;
 r = _downAp3( centerIJK.i, centerIJK.j, centerIJK.k );
 r = _downAp3r( r.i, r.j, r.k );
 centerIJK.i = r.i; 
 centerIJK.j = r.j; 
 centerIJK.k = r.k;
 
 adjRes = res;

 if (isResClassIII(res)) then
  r = _downAp7r( centerIJK.i, centerIJK.j, centerIJK.k );
  centerIJK.i = r.i; 
  centerIJK.j = r.j; 
  centerIJK.k = r.k;
  adjRes = adjRes + 1;
 end if;

 for i in 1..NUM_PENT_VERTS loop

  cp.face = centerIJK.face;
 
  r = _ijkNormalize( centerIJK.i + verts[i][1], centerIJK.j + verts[i][2], centerIJK.k + verts[i][3] );
  cp.i = r.i;
  cp.j = r.j;
  cp.k = r.k;
  fijkVerts[i] = cp;

 end loop;

 g_numVers = 0;

 for vert in 0..NUM_PENT_VERTS loop

  v = vert % NUM_PENT_VERTS;

  fijk = fijkVerts[ v+1 ];

  pentLeading4 = false;
  
  r1 = _adjustOverageClassII( fijk, adjRes, pentLeading4, true );
  r1a = r1.fijk;
  fijk.face = r1a.face; 
  fijk.i    = r1a.i; 
  fijk.j    = r1a.j;
  fijk.k    = r1a.k;

  if ( r1.overage = 2 ) then
  -- for abc in 1..5 /*dbg*/
   loop

     r1  = _adjustOverageClassII( fijk, adjRes, false, true );
     -- RAISE NOTICE '%, fijk(%) adjRes(%) r1(%)', abc, fijk, adjRes, r1;
     r1a = r1.fijk;
     fijk.face = r1a.face; 
     fijk.i    = r1a.i; 
     fijk.j    = r1a.j;
     fijk.k    = r1a.k;
    
     if ( r1.overage != 2 ) then
      exit;
     end if;
   end loop;
  end if;
  
  if ( isResClassIII(res) and vert > 0) then
   
   tmpFijk = fijk;
   orig2d0 = _ijkToHex2d(lastFijk.i, lastFijk.j, lastFijk.k );
   
   currentToLastDir = adjacentFaceDir(tmpFijk.face,lastFijk.face);
   fijkOrient = faceNeighbors(tmpFijk.face,currentToLastDir);
   
   tmpFijk.face = fijkOrient.face;

  --RAISE NOTICE '%, fijkOrient(%) adjRes(%)', 1, fijkOrient, adjRes;

  for i in 0..(fijkOrient.ccwRot60 - 1) loop
   tmpIJK = _ijkRotate60ccw(tmpFijk.i,tmpFijk.j,tmpFijk.k);
   tmpFijk.i = tmpIJK.i;
   tmpFijk.j = tmpIJK.j;
   tmpFijk.k = tmpIJK.k;
  end loop;

  transVec = _ijkScale(fijkOrient.i, fijkOrient.j, fijkOrient.k, unitScaleByCIIres(adjRes) * 3);
  transVec = _ijkNormalize(transVec.i + tmpFijk.i, transVec.j + tmpFijk.j, transVec.k + tmpFijk.k ); 
  tmpFijk.i = transVec.i;
  tmpFijk.j = transVec.j;
  tmpFijk.k = transVec.k;
  
  orig2d1 = _ijkToHex2d(tmpFijk.i, tmpFijk.j, tmpFijk.k );

  maxDim = maxDimByCIIres(adjRes);

  case adjacentFaceDir(tmpFijk.face,fijk.face)
   when 1 then --IJ 
     edge0 = array [3.0 * maxDim, 0.0];
     edge1 = array [-1.5 * maxDim, 3.0 * M_SQRT3_2 * maxDim];
   when 3 then --JK
     edge0 = array [-1.5 * maxDim, 3.0 * M_SQRT3_2 * maxDim];
     edge1 = array [-1.5 * maxDim, -3.0 * M_SQRT3_2 * maxDim];
   when 2 then --KI
     edge0 = array [-1.5 * maxDim, -3.0 * M_SQRT3_2 * maxDim];
     edge1 = array [3.0 * maxDim, 0.0];
   else
     RAISE EXCEPTION 'adjacentFaceDir(% ,%) not in [1,2,3]', tmpFijk.face, fijk.face;
  end case;

  inter = _v2dIntersect(orig2d0.x,orig2d0.y, 
                        orig2d1.x,orig2d1.y,
                        edge0[1], edge0[2],
                        edge1[1], edge1[2] );
  pnt = _hex2dToGeo(inter.x, inter.y,tmpFijk.face, adjRes, true );

  arr_pts = array_append( arr_pts, st_point(pnt.lon,pnt.lat));
  g_numVers = g_numVers+1; --formalism

  end if;
  
  if ( vert < NUM_PENT_VERTS) then
   vec = _ijkToHex2d(fijk.i, fijk.j, fijk.k );
   pnt = _hex2dToGeo(vec.x, vec.y, fijk.face, adjRes, true );
   arr_pts = array_append( arr_pts, st_point(pnt.lon,pnt.lat));
   g_numVers = g_numVers+1; --formalism
  end if;

  lastFijk = fijk;

 end loop;
  
 return st_makepolygon( st_makeline( array_append( arr_pts, arr_pts[1] ) ) ); -- append first point to close linestring for polygon

end;
$body$ 
language plpgsql immutable;

create or replace function _faceIjkToGeoBoundary( h face_ijk_t, res integer, pentagon boolean)
returns geometry as
$body$
declare
 M_SQRT3_2 CONSTANT double precision := 0.8660254037844386467637231707529361834714;
 NUM_HEX_VERTS CONSTANT integer := 6;
 verts integer [6][3];
 adjRes integer;
 centerIJK face_ijk_t;
 r record;
 r1 record;
 r1a record;
 orig2d0 record;
 orig2d1 record;
 fijkVerts face_ijk_t[6];
 cp face_ijk_t;
 fijk face_ijk_t;
 lastFace integer;
 lastOverage integer;
 g_numVers integer;
 lastVfijk face_ijk_t;
 vfijk face_ijk_t;
 lastV integer;
 isIntersectionAtVertex boolean;
 v integer;
 pentLeading4 boolean;
 overage integer;
 maxDim integer;
 edge0 double precision[2];
 edge1 double precision[2];
 face2 integer;
 inter record;
 arr_pts geometry[];
 pnt record;
 vec record;
 --aFlag boolean := false;
begin

 if pentagon then
  return _faceIjkPentToGeoBoundary( h, res); 
 end if;

 if (isResClassIII(res)) then
   verts = array 
    [
        [5, 4, 0],  -- 0
        [1, 5, 0],  -- 1
        [0, 5, 4],  -- 2
        [0, 1, 5],  -- 3
        [4, 0, 5],  -- 4
        [5, 0, 1]   -- 5
    ];
 else
   verts = array
    [
        [2, 1, 0],  -- 0
        [1, 2, 0],  -- 1
        [0, 2, 1],  -- 2
        [0, 1, 2],  -- 3
        [1, 0, 2],  -- 4
        [2, 0, 1]   -- 5
    ];
 end if;

 centerIJK = h;
 r = _downAp3( centerIJK.i, centerIJK.j, centerIJK.k );
 r = _downAp3r( r.i, r.j, r.k );
 centerIJK.i = r.i; 
 centerIJK.j = r.j; 
 centerIJK.k = r.k;
 
 adjRes = res;

 if (isResClassIII(res)) then
  r = _downAp7r( centerIJK.i, centerIJK.j, centerIJK.k );
  centerIJK.i = r.i; 
  centerIJK.j = r.j; 
  centerIJK.k = r.k;
  adjRes = adjRes + 1;
 end if;

 for i in 1..NUM_HEX_VERTS loop

  cp.face = centerIJK.face;
 
  r = _ijkNormalize( centerIJK.i + verts[i][1], centerIJK.j + verts[i][2], centerIJK.k + verts[i][3] );
  cp.i = r.i;
  cp.j = r.j;
  cp.k = r.k;
  fijkVerts[i] = cp;

 end loop;

 g_numVers = 0;
 lastFace = -1;
 lastOverage = 0;  -- 0: none; 1: edge; 2: overage

 for vert in 0..NUM_HEX_VERTS loop

  v = vert % NUM_HEX_VERTS;

  fijk = fijkVerts[ v+1 ];

  pentLeading4 = false;

  r1 = _adjustOverageClassII( fijk, adjRes, pentLeading4, true );
  overage = r1.overage;
  r1a = r1.fijk;
  fijk.face = r1a.face; 
  fijk.i    = r1a.i; 
  fijk.j    = r1a.j;
  fijk.k    = r1a.k;


  if ( isResClassIII(res) and vert > 0 and fijk.face != lastFace and lastOverage != 1 ) then

   lastV = (v + 5) % NUM_HEX_VERTS;

   lastVfijk = fijkVerts[lastV+1];
   vfijk = fijkVerts[v+1];

   orig2d0 = _ijkToHex2d(lastVfijk.i,lastVfijk.j,lastVfijk.k);
   orig2d1 = _ijkToHex2d(vfijk.i, vfijk.j, vfijk.k ); 

   maxDim = maxDimByCIIres(adjRes);
   
   if( lastFace = centerIJK.face ) then
    face2 = fijk.face;
   else
    face2 = lastFace;
   end if;

   case adjacentFaceDir(centerIJK.face,face2)
    when 1 then --IJ 
      edge0 = array [3.0 * maxDim, 0.0];
      edge1 = array [-1.5 * maxDim, 3.0 * M_SQRT3_2 * maxDim];
    when 3 then --JK
      edge0 = array [-1.5 * maxDim, 3.0 * M_SQRT3_2 * maxDim];
      edge1 = array [-1.5 * maxDim, -3.0 * M_SQRT3_2 * maxDim];
    when 2 then --KI
      edge0 = array [-1.5 * maxDim, -3.0 * M_SQRT3_2 * maxDim];
      edge1 = array [3.0 * maxDim, 0.0];
    else
      RAISE EXCEPTION 'adjacentFaceDir(% ,%) not in [1,2,3]', centerIJK.face,face2;
   end case;
  
   inter = _v2dIntersect(orig2d0.x,orig2d0.y, 
                         orig2d1.x,orig2d1.y,
                         edge0[1], edge0[2],
                         edge1[1], edge1[2] );

   --RAISE NOTICE 'xy0(%) xy1(%)', orig2d0, orig2d1;

   isIntersectionAtVertex = ( _v2dEquals(orig2d0.x,orig2d0.y,inter.x, inter.y) or _v2dEquals(orig2d1.x,orig2d1.y,inter.x, inter.y) );

   if( not isIntersectionAtVertex ) then
    pnt = _hex2dToGeo(inter.x, inter.y, centerIJK.face, adjRes, true );
/* 
    if ( g_numVers = 0 ) then
     if ( pnt.lon > 0 ) then
      aFlag = true;
     end if;
    elsif ( pnt.lon > 0 and aFlag = false ) then 
      pnt.lon = -pi() - (pi() - pnt.lon);
    elsif ( pnt.lon < 0 and aFlag = true ) then 
      pnt.lon = pi() - (-pi() - pnt.lon);
    end if;
*/
    arr_pts = array_append( arr_pts, st_point(pnt.lon,pnt.lat));
    g_numVers = g_numVers+1; --formalism
    --RAISE NOTICE '!isIntersectionAtVertex %, xy(%) face(%) adjRes(%)', g_numVers, inter, centerIJK.face, adjRes;
   end if;
  end if;

  if (vert < NUM_HEX_VERTS) then
   vec = _ijkToHex2d(fijk.i, fijk.j, fijk.k );
   pnt = _hex2dToGeo(vec.x, vec.y, fijk.face, adjRes, true );
/*
    if ( g_numVers = 0 ) then
     if ( pnt.lon > 0 ) then
      aFlag = true;
     end if;
    elsif ( pnt.lon > 0 and aFlag = false ) then 
      pnt.lon = -pi() - (pi() - pnt.lon);
    elsif ( pnt.lon < 0 and aFlag = true ) then 
      pnt.lon = pi() - (-pi() - pnt.lon);
    end if;
*/
   arr_pts = array_append( arr_pts, st_point(pnt.lon,pnt.lat));
   g_numVers = g_numVers+1; --formalism
   --RAISE NOTICE 'vert %, xy(%) face(%) adjRes(%)', g_numVers, vec, fijk.face, adjRes;
  end if;

  lastFace = fijk.face;
  lastOverage = overage;

 end loop;

 return st_makepolygon( st_makeline( array_append( arr_pts, arr_pts[1] ) ) ); -- append first point to close linestring for polygon

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToGeoBoundary( h3 H3Index ) 
returns geometry as
$body$
declare
 fijk face_ijk_t;
begin

 fijk = _h3ToFaceIjk( h3 );
 return _faceIjkToGeoBoundary( fijk, H3_GET_RESOLUTION(h3), h3IsPentagon(h3) );

end;
$body$ 
language plpgsql immutable;

create or replace function _convert_on_dateline( geo geometry )
returns geometry as
$body$
declare
 r record;
 g geometry;
begin
         -- assers simpl polygon as input
 select  sum( case x >= 150.0 when true then 1 else 0 end ) as x_gte0, sum( case x < -150.0 when true then 1 else 0 end  ) as x_lt0 into strict r
 from ( select st_x(geom) as x from ( select ( st_dumppoints( geo )).geom ) dp ) dp1;

 if ( (r.x_gte0 = 0) or ( r.x_lt0 = 0 ) ) then
  return geo;
 end if;

 if ( r.x_gte0 < r.x_lt0 ) then -- map x : 177.0 -> -183.00
   select st_makepolygon( st_makeline( st_point( case x >= 0 when true then x - 360.0 else x end, dp1.y ) ) ) into g
   from
   ( select st_y(geom) as y, st_x(geom) as x from ( select ( st_dumppoints( geo )).geom ) dp ) dp1;
   return g;
 else -- map x : -177.0 -> 183.00
   select st_makepolygon( st_makeline( st_point( case x < 0 when true then  x + 360.0 else x end, dp1.y ) ) ) into g
   from
   ( select st_y(geom) as y, st_x(geom) as x from ( select ( st_dumppoints( geo )).geom ) dp ) dp1;
   return g;
 end if;

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToGeoBoundaryDeg_p( h3 H3Index ) 
returns geometry as
 $body$
  select ST_SetSRID( _convert_on_dateline( st_scale( h3ToGeoBoundary( h3), 180.0/pi(), 180.0/pi() )),4326) as geo
 $body$
language sql immutable;

create or replace function h3ToGeoBoundaryDeg( h3 H3Index ) 
returns geometry as
$body$
declare
 hg geometry;
begin

 select t.geo into hg from h3cache t where t.h3 = h3ToGeoBoundaryDeg.h3 limit 1;

 if( hg notnull ) then
  return hg;
 end if;

 hg = h3ToGeoBoundaryDeg_p( h3 );
 
 insert into h3cache ( h3, res, geo ) values( h3, H3_GET_RESOLUTION(h3), hg );

 return hg; 
 
end;
$body$
language plpgsql volatile;



create or replace function  h3NeighborRotations( origin H3Index, dir integer, inout rotations integer, out h3out H3Index) as
$body$
declare
 INVALID_BASE_CELL CONSTANT integer := 127;
 H3_INVALID_INDEX CONSTANT integer := 0;
 CENTER_DIGIT CONSTANT integer := 0;
 K_AXES_DIGIT CONSTANT integer := 1;
 JK_AXES_DIGIT CONSTANT integer := 3;
 IK_AXES_DIGIT CONSTANT integer := 5;
 h3 H3Index;
 newRotations integer := 0;
 oldBaseCell integer;
 r_oldBaseCell record;
 oldLeadingDigit integer;
 r integer;
 oldDigit integer;
 nextDir integer;
 newBaseCell integer;
 alreadyAdjustedKSubsequence boolean := false;
begin

  h3out = origin;
  for i in 0..(rotations -1) loop
   dir = _rotate60ccw(dir);
  end loop;

  oldBaseCell = H3_GET_BASE_CELL(h3out);
  oldLeadingDigit = _h3LeadingNonZeroDigit(h3out);
  r = H3_GET_RESOLUTION(h3out) - 1;
  
  loop
   if (r = -1) then
    h3out = H3_SET_BASE_CELL(h3out, baseCellNeighbors(oldBaseCell,dir));
    newRotations = baseCellNeighbor60CCWRots(oldBaseCell,dir);
    
    if (H3_GET_BASE_CELL(h3out) = INVALID_BASE_CELL) then

     h3out = H3_SET_BASE_CELL(h3out, baseCellNeighbors(oldBaseCell,IK_AXES_DIGIT));
     newRotations = baseCellNeighbor60CCWRots(oldBaseCell,IK_AXES_DIGIT);
     h3out = _h3Rotate60ccw(h3out);
     rotations = rotations + 1;

    end if;

    exit;
    
   else

    oldDigit = H3_GET_INDEX_DIGIT(h3out, r + 1);    

    if (isResClassIII(r + 1)) then
     h3out = H3_SET_INDEX_DIGIT(h3out, r + 1, NEW_DIGIT_II(oldDigit,dir));
     nextDir = NEW_ADJUSTMENT_II(oldDigit,dir);
    else 
     h3out = H3_SET_INDEX_DIGIT(h3out, r + 1, NEW_DIGIT_III(oldDigit,dir));
     nextDir = NEW_ADJUSTMENT_III(oldDigit,dir);
    end if;

    if (nextDir != CENTER_DIGIT) then
     dir = nextDir;
     r = r-1;
    else 
      exit;
    end if;

   end if;
  end loop;

  newBaseCell = H3_GET_BASE_CELL(h3out);
  
  if( _isBaseCellPentagon(newBaseCell) ) then 
   alreadyAdjustedKSubsequence = false;
   if (_h3LeadingNonZeroDigit(h3out) = K_AXES_DIGIT) then
    if (oldBaseCell != newBaseCell) then

     r_oldBaseCell = baseCellData(oldBaseCell);

     if (_baseCellIsCwOffset( newBaseCell, r_oldBaseCell.face)) then
      h3out = _h3Rotate60cw(h3out);
     else 
      h3out = _h3Rotate60ccw(h3out);
     end if; 
     alreadyAdjustedKSubsequence = true;

    else

     if (oldLeadingDigit = CENTER_DIGIT) then
        h3out = H3_INVALID_INDEX;
        return;
     elsif (oldLeadingDigit = JK_AXES_DIGIT) then
        h3out = _h3Rotate60ccw(h3out);
        rotations = rotations + 1;
     elsif (oldLeadingDigit = IK_AXES_DIGIT) then
        h3out = _h3Rotate60cw(h3out);
        rotations = rotations + 5;
     else 
        h3out = H3_INVALID_INDEX;
        return;
     end if;

    end if;
   end if;

   for i in 0..(newRotations-1) loop
    h3out = _h3RotatePent60ccw(h3out);
   end loop; 

   if (oldBaseCell != newBaseCell) then
    if (_isBaseCellPolarPentagon(newBaseCell)) then
     if ( (oldBaseCell != 118) and (oldBaseCell != 8) and (_h3LeadingNonZeroDigit(h3out) != JK_AXES_DIGIT)) then
      rotations = rotations + 1;
     end if;
    elsif ( (_h3LeadingNonZeroDigit(h3out) = IK_AXES_DIGIT) and ( not alreadyAdjustedKSubsequence )) then
     rotations = rotations + 1;
    end if;
   end if;

  else
    for i in 0..(newRotations-1) loop
     h3out = _h3Rotate60ccw(h3out);
    end loop; 
  end if;

  rotations = (rotations + newRotations) % 6;
  --return h3out;
end;
$body$ 
language plpgsql immutable;

create or replace function maxKringSize(k integer) 
returns integer as
 $body$
  select 3 * k * (k + 1) + 1
 $body$
language sql immutable;

create or replace function hexRangeDistances( origin H3Index, k integer ) 
  returns table ( h3 H3Index, distance integer ) as
$body$
declare
 NEXT_RING_DIRECTION CONSTANT integer := 4;
 arr_out bigint[];
 arr_distance integer[];
 idx integer := 1;
 ring integer := 1;
 direction integer := 0;
 i integer := 0;
 rotations integer := 0;
 r record;
begin

 arr_out[idx] = origin;
 arr_distance[idx] = 0;
 idx = idx+1;

 if ( h3IsPentagon(origin) ) then
  raise exception 'HEX_RANGE_PENTAGON h3(%)', to_hex(origin) USING ERRCODE = '02000'; --no_data
 end if; 

 while ( ring <= k ) loop

  if ( (direction = 0) and (i = 0) ) then

   -- RAISE NOTICE 'in origin(%) rot(%)', to_hex(origin), rotations;

   r = h3NeighborRotations(origin, NEXT_RING_DIRECTION, rotations);
   origin = r.h3out;
   rotations = r.rotations;

   -- RAISE NOTICE 'r %, origin(%) rot(%)', r, origin, rotations;

   if ( origin = 0 ) then 
    raise exception 'HEX_RANGE_K_SUBSEQUENCE h3(%)', to_hex(origin) USING ERRCODE = '02000'; --no_data
   end if;
    
   if ( h3IsPentagon( origin ) ) then
    raise exception 'HEX_RANGE_PENTAGON h3(%)', to_hex(origin) USING ERRCODE = '02000'; --no_data
   end if; 
  end if;

  r = h3NeighborRotations(origin, DIRECTIONS(direction), rotations);
  origin = r.h3out;
  rotations = r.rotations;
  
  if ( origin = 0 ) then 
   raise exception 'HEX_RANGE_K_SUBSEQUENCE h3(%)', to_hex(origin) USING ERRCODE = '02000'; --no_data
  end if;

  arr_out[idx] = origin;
  arr_distance[idx] = ring;
  idx = idx +1;

  i = i +1;

  if (  i = ring ) then
   i = 0;
   direction = direction + 1;
   if ( direction = 6 ) then
    direction = 0;
    ring = ring +1;
   end if;
  end if;
   
  if ( h3IsPentagon( origin ) ) then
   raise exception 'HEX_RANGE_PENTAGON h3(%)', to_hex(origin) USING ERRCODE = '02000'; --no_data
  end if; 

 end loop; 

 return query select u.h3::H3Index, u.distance from unnest( arr_out, arr_distance ) u( h3, distance );

end;
$body$ 
language plpgsql immutable;

create or replace function _kRingInternal_r( origin H3Index, k integer, maxIdx integer, curK integer, inout arr_out bigint[], inout arr_distance integer[] ) 
 as
$body$
declare
 offs integer;
 rotations integer := 0;
 r1 record;
 r2 record;
begin

 if origin = 0 then 
  return;
 end if;

 offs = ( origin % maxIdx ) +1;

 --RAISE NOTICE 'in origin(%) k(%) maxIdx(%) curK(%) offs(%) out(%) distance(%)', origin, k, maxIdx, curK, offs, arr_out, arr_distance; 


 while ( ( arr_out[offs] != 0 ) and ( arr_out[offs] != origin )) loop
    offs = ((offs + 1) % maxIdx) +1;
 end loop;

 if ((arr_out[offs] = origin) and  (arr_distance[offs] <= curK )) then
  return;
 end if; 

 arr_out[offs] = origin;
 arr_distance[offs] = curK;

 if (curK >= k) then
  return;
 end if;
 
 for direction in 0..5 loop

  -- rotations = 0;
  r1 = h3NeighborRotations(origin, DIRECTIONS(direction), rotations);

  --RAISE NOTICE 'origin(%) dir(%) rotation(%) -> origin(%) rotation(%)', to_hex(origin), direction, rotations, to_hex(r1.h3out), r1.rotations;

  r2 = _kRingInternal_r( r1.h3out,  k, maxIdx, curK + 1, arr_out, arr_distance );

  arr_out = r2.arr_out;
  arr_distance = r2.arr_distance;

 end loop;  

end;
$body$ 
language plpgsql immutable;


create or replace function _kRingInternal( origin H3Index, k integer, maxIdx integer, curK integer ) 
  returns table ( h3 H3Index, distance integer ) as
$body$
declare
 arr_out bigint[]; 
 arr_distance integer[];
 r record;
begin
 
 arr_out = array_fill( 0::bigint, ARRAY[maxIdx]);
 arr_distance = array_fill( 0::integer, ARRAY[maxIdx]);

 r = _kRingInternal_r( origin, k, maxIdx , curK, arr_out, arr_distance ) ;

 return query select u.h3::H3Index, u.distance from unnest( r.arr_out, r.arr_distance ) u( h3, distance );

end;
$body$ 
language plpgsql immutable;

create or replace function kRingDistances( origin H3Index, k integer ) 
  returns table ( h3 H3Index, distance integer ) as
$body$
declare
begin

  return query select o.h3, o.distance from hexRangeDistances(origin, k ) o;

 exception
  when sqlstate '02000' then --no_data
   return query select o.h3, o.distance from _kRingInternal(origin, k, maxKringSize(k), 0) o;

end;
$body$ 
language plpgsql immutable;


create or replace function kRing( origin H3Index, k integer ) 
 returns table ( h3 H3Index ) as
$body$
declare
begin
 return query select krd.h3 from kRingDistances( origin, k ) krd;
end;
$body$ 
language plpgsql immutable;


create or replace function _faceIjkToGeo( h face_ijk_t, res integer, out lat double precision, out lon double precision )
as
$body$
declare
 vec record;
 pnt record;
begin

  vec = _ijkToHex2d(h.i, h.j, h.k );
  pnt = _hex2dToGeo(vec.x, vec.y, h.face, res, false );
  lon = pnt.lon;
  lat = pnt.lat;

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToGeo( h3 H3Index, out lat double precision, out lon double precision )
as
$body$
declare
 fijk face_ijk_t;
 pnt record;
begin

 fijk = _h3ToFaceIjk( h3 );
 pnt = _faceIjkToGeo( fijk, H3_GET_RESOLUTION(h3) );
 
 lon = pnt.lon;
 lat = pnt.lat;

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToGeoDeg( h3 H3Index )
 returns geometry as
$body$
declare
 pnt record;
begin

 pnt = h3ToGeo( h3 );
 return st_point( degrees(pnt.lon), degrees(pnt.lat));

end;
$body$ 
language plpgsql immutable;


create or replace function _hexRadiusKm( h3 H3Index ) 
 returns double precision as
$body$
declare
 pnt record;
 g geometry;
begin

 pnt = h3ToGeo( h3 );

 select (ST_DumpPoints( h3ToGeoBoundaryDeg( h3 ) )).geom into g limit 1;

 return ST_DistanceSphere( g, st_scale( st_point(pnt.lon,pnt.lat), 180.0/pi(), 180.0/pi() ) ) / 1000.0;
 --return ST_DistanceSpheroid( g, ST_SetSRID( st_scale( st_point(pnt.lon,pnt.lat), 180.0/pi(), 180.0/pi() ), 4326 ), 'SPHEROID["WGS 84",6378137,298.257223563]' ) / 1000.0;
 
end;
$body$ 
language plpgsql immutable;

create or replace function bboxHexRadiusDeg( bbox geometry, res integer ) 
returns integer as
$body$
declare
 bboxRadiusKm double precision;
 centerHexRadiusKm double precision;
 c geometry;
 h3 H3Index;
begin

 bboxRadiusKm = st_length( ST_BoundingDiagonal( bbox )::geography ) / 2000.0;

 c = ST_Centroid(bbox);
 h3 = geoToH3(radians(st_y(c)), radians(st_x(c)), res);

 centerHexRadiusKm = _hexRadiusKm( h3 );

 return ceil( bboxRadiusKm / (1.5 * centerHexRadiusKm) )::integer; 

end;
$body$ 
language plpgsql immutable;

create or replace function polyfillDeg_s( geoPolygon geometry, res integer ) 
 returns table ( h3 H3Index ) as
$body$
declare
 minK integer;
 bbox geometry;
 center geometry;
 centerH3 H3Index;
begin

 bbox = st_envelope(geoPolygon);
 minK = bboxHexRadiusDeg( bbox , res);

 center = st_centroid( bbox );

 centerH3 = geoToH3(radians(st_y(center)), radians(st_x(center)), res);

 if( minK > 2 ) then

  return query    -- paint the boundary - outline and holes
   select cl.h3 from coveringDeg( ST_Boundary( geoPolygon ) , res ) cl;
   
  return query   -- paint the interior
   select kr.h3 from kRing(centerH3, minK ) kr
   where 1 = 1
     and ST_Within(ST_SetSRID(h3ToGeoDeg( kr.h3 ),4326), geoPolygon);

 else
  return query
   select kr.h3 from kRing(centerH3, minK ) kr
   where 1 = 1
     and ST_Intersects(ST_SetSRID(h3ToGeoBoundaryDeg( kr.h3 ),4326), ST_SetSRID(geoPolygon,4326));
 end if;    
 
end;
$body$ 
language plpgsql immutable;

create or replace function polyfillDeg( geoPolygon geometry, res integer, subdivide boolean default true ) 
 returns table ( h3 H3Index ) as
$body$
declare
 max_vertices integer := 16 * 1024;
begin

 if subdivide then 
  return query
   select polyfillDeg_s( st_subdivide( st_force2d( geoPolygon ), max_vertices ), res );
 else  
  return query
   select  polyfillDeg_s( geoPolygon, res );
 end if; 

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToParent( h H3Index, parentRes integer)
returns H3Index as
$body$
declare
 H3_INVALID_INDEX constant integer := 0;
 MAX_H3_RES constant integer := 15;
 H3_DIGIT_MASK integer := 7;
 childRes integer;
 parentH H3Index;
begin

 childRes = H3_GET_RESOLUTION(h);

 if (parentRes > childRes) then
     return H3_INVALID_INDEX;
 elsif (parentRes = childRes) then
     return h;
 elsif ((parentRes < 0) or (parentRes > MAX_H3_RES)) then
     return H3_INVALID_INDEX;
 end if;

 parentH = H3_SET_RESOLUTION(h, parentRes);

 for i in (parentRes + 1)..childRes loop
  parentH = H3_SET_INDEX_DIGIT(parentH, i, H3_DIGIT_MASK);
 end loop;

 return parentH;

end;
$body$ 
language plpgsql immutable;

create or replace function h3ToParent( h H3Index )
returns H3Index as
 $body$
  select h3ToParent(h, H3_GET_RESOLUTION(h)-1)
 $body$  
language sql immutable;

create or replace function compact( h3List bigint[] ) 
 returns table ( h3 H3Index ) as
$body$ 
 with recursive search_graph( h3, res, parent, pqty ) as
 ( select i.h3, H3_GET_RESOLUTION(i.h3)as res, ARRAY[0,h3ToParent(i.h3)] as parent, count(1) OVER (PARTITION BY ( h3ToParent(i.h3) )) as pqty 
   from ( select distinct h as h3 from unnest(h3List) h ) i 
  union all
   select iii.*, count(1) OVER (PARTITION BY iii.parent) as pqty 
   from
   ( select distinct on (ii.parent) ii.parent[2] as h3, H3_GET_RESOLUTION(ii.parent[2]) as res, ARRAY[ ii.parent[1]+1, h3ToParent(ii.parent[2])] as parent
     from search_graph ii
     where 1 = 1
       and ii.pqty = 7 
   ) iii
 )
 select h3::H3Index from search_graph
 where 1 = 1
  and pqty < 7
$body$ 
language sql immutable;

create or replace function polyfillCompactDeg( geoPolygon geometry, res integer ) 
 returns table ( h3 H3Index ) as
 $body$
 select h3 from compact( ARRAY( select h::bigint from polyfillDeg( geoPolygon , res) h ) )
 $body$
language sql immutable;

create or replace function makeDirectChild( h H3Index, cellNumber integer ) 
 returns H3Index as
$body$
declare
 childRes integer := H3_GET_RESOLUTION(h) +1;
begin
  return H3_SET_INDEX_DIGIT( H3_SET_RESOLUTION(h, childRes), childRes, cellNumber);
end;
$body$ 
language plpgsql immutable;

create or replace function h3ToChildren( h H3Index, childRes integer ) 
 returns table ( h3 H3Index ) as
$body$
declare
 K_AXES_DIGIT CONSTANT integer := 1;
 parentRes integer := H3_GET_RESOLUTION(h);
 isAPentagon boolean := h3IsPentagon(h);
begin
 
 if parentRes > childRes then
  return;
 elsif parentRes = childRes then
  h3 = h;
  return next;
 else
  for i in 0..6 loop
   if ( (isAPentagon) and ( i = K_AXES_DIGIT ) ) then
    null;
   else
    return query select h3ToChildren( makeDirectChild(h,i), childRes );
   end if;
  end loop;
 end if; 
end;
$body$ 
language plpgsql immutable;

create or replace function uncompact( h3List bigint[], res integer ) 
 returns table ( h3 H3Index ) as
'select h3ToChildren( h, res ) from unnest(h3List) h' 
language sql immutable;

create or replace function hexAreaKm2( res integer  )
 returns double precision as
$body$
declare
 arrDim double precision[] = array
[
        4250546.848, 607220.9782, 86745.85403, 12392.26486,
        1770.323552, 252.9033645, 36.1290521,  5.1612932,
        0.7373276,   0.1053325,   0.0150475,   0.0021496,
        0.0003071,   0.0000439,   0.0000063,   0.0000009
];
begin
 
 res = res + 1;
 return arrDim[res]; 

end;
$body$
language plpgsql immutable;

create or replace function hexAreaM2( res integer  )
 returns double precision as
$body$
declare
 arrDim double precision[] = array
[
        4.25055E+12, 6.07221E+11, 86745854035, 12392264862,
        1770323552,  252903364.5, 36129052.1,  5161293.2,
        737327.6,    105332.5,    15047.5,     2149.6,
        307.1,       43.9,        6.3,         0.9
];
begin
 
 res = res + 1;
 return arrDim[res]; 

end;
$body$
language plpgsql immutable;

create or replace function edgeLengthKm( res integer  )
 returns double precision as
$body$
declare
 arrDim double precision[] = array
[
        1107.712591, 418.6760055, 158.2446558, 59.81085794,
        22.6063794,  8.544408276, 3.229482772, 1.220629759,
        0.461354684, 0.174375668, 0.065907807, 0.024910561,
        0.009415526, 0.003559893, 0.001348575, 0.000509713
];
begin
 
 res = res + 1;
 return arrDim[res]; 

end;
$body$
language plpgsql immutable;

create or replace function edgeLengthM( res integer  )
 returns double precision as
$body$
declare
 arrDim double precision[] = array
[
        1107712.591, 418676.0055, 158244.6558, 59810.85794,
        22606.3794,  8544.408276, 3229.482772, 1220.629759,
        461.3546837, 174.3756681, 65.90780749, 24.9105614,
        9.415526211, 3.559893033, 1.348574562, 0.509713273
];
begin
 
 res = res + 1;
 return arrDim[res]; 

end;
$body$
language plpgsql immutable;


create or replace function _line_coords_seq( line geometry, res integer ) 
 returns table ( nr integer, h3 H3Index, lead boolean, geo geometry ) as
$body$
 select nr, h3, lead( true::boolean,1, false::boolean ) over (partition by h3 order by nr), geom as geo
 from 
 ( select nr, h3, ( lag( 1::integer ) over (partition by h3 order by nr) + lead( 1::integer ) over (partition by h3 order by nr) ) as mflag, geom
   from 
   ( 
    select (d).path[1] as nr, (d).geom, geotoh3deg( (d).geom, res ) as h3
    from
    ( select st_dumppoints(line) d  ) o    
   ) oo
 ) ooo
 where 1 = 1
   and mflag is null  
-- order by nr
$body$ 
language sql immutable;

create or replace function _walkpath_s( h_start H3Index, p_start geometry, h_end H3Index, p_end geometry, res integer, edge_len_m double precision ) 
 returns table ( h3 H3Index ) as
$body$
declare
 h H3Index;
 p geometry;
 l geometry;
begin

 if ((h_end notnull) and (h_start notnull)) then 
  
  if h_start = h_end then
   return query select h_start;
  else
  
   l = ST_MakeLine(p_start, p_end);
   
   if( ST_Length(l::geography) < edge_len_m ) then
    return query select h_start union select h_end;
   else 
    p = ST_LineInterpolatePoint( l, 0.5 );
    h = geotoh3deg( p, res );

    -- RAISE NOTICE 'h %, p(%), len(%) ', h, st_astext(p),ST_Length(ST_MakeLine(p_start, p_end)::geography);
    if( h != h_start ) then
     return query select _walkpath_s( h_start, p_start, h, p, res, edge_len_m );
    end if; 

    if( h != h_end ) then
     return query select _walkpath_s( h, p, h_end, p_end, res, edge_len_m );
    end if; 
     
   end if;
  end if;
 end if;
end;
$body$ 
language plpgsql immutable;

create or replace function walkpath( h_start H3Index, p_start geometry, h_end H3Index, p_end geometry, res integer ) 
 returns table ( h3 H3Index ) as
$body$
 select distinct _walkpath_s(h_start, p_start, h_end, p_end, res, edgeLengthM( res ) )
$body$ 
language sql immutable;

create or replace function coveringLineDeg( line geometry, res integer ) 
 returns table ( h3 H3Index ) as
$body$
 select distinct walkpath(h3_start,p_start,h3_end,p_end,res) 
 from( select nr, lead, h3 as h3_start, geo as p_start, lead( h3 ) over ( order by nr) as h3_end, lead( geo ) over ( order by nr) as p_end from _line_coords_seq( line ,res) order by 1 ) o  
$body$ 
language sql immutable;

create or replace function coveringDeg( geo geometry, res integer ) 
 returns table ( h3 H3Index ) as
$body$
begin
 case ST_GeometryType(geo)
  when 'ST_Point' then
   return query select geotoh3deg( geo, res );
  when 'ST_MultiPoint' then 
   return query select distinct geotoh3deg( (st_dump(geo)).geom, res );
  when 'ST_LineString' then
   return query select coveringLineDeg( geo, res );
  when 'ST_MultiLineString' then 
   return query select distinct coveringLineDeg( (st_dump(geo)).geom, res );
  when 'ST_Polygon' then
   return query select polyfillDeg( geo, res );
  when 'ST_MultiPolygon' then 
   return query select distinct polyfillDeg( (st_dump(geo)).geom, res );
  else 
   return query select geotoh3deg( st_closestpoint( geo , geo ), res );
 end case;  
end;
$body$ 
language plpgsql immutable;

create or replace function zoom2resolution( zoom integer ) 
returns integer as  
 'select greatest( round(( zoom*ln(4) )/ ln(7))::integer -2 , 0 )
 '
language sql immutable;

create or replace function lonlat2pxpy( lon double precision, lat double precision, pow2_z_x_pxsz bigint, out px bigint, out py bigint )
as 
$body$   -- pow2_z_x_pxsz = power(2, ( zoom )) * pixelsize
 select 
      (case
        when lon < -180 then 0
        when lon > 180  then pow2_z_x_pxsz
        else floor((lon + 180.0) / 360.0 * pow2_z_x_pxsz ) 
       end
      )::bigint,
      (case 
        when lat <= -85 then pow2_z_x_pxsz
        when lat >= 85  then 0
        else floor((1.0 - ln(tan(radians(lat)) + (1.0 / cos(radians(lat)))) / pi()) / 2 * pow2_z_x_pxsz )
       end
      )::bigint
$body$
language sql immutable;

/*  Tests 

select lat, lon, res, 
       (_geoToHex2d( radians( i.lat ), radians( i.lon ), i.res )).* , 
       (_geoToFaceIjk( radians( i.lat ), radians( i.lon ), i.res )).*,
       (geoToH3( radians( i.lat ), radians( i.lon ), i.res ))::bit(64)
from ( select 40.689167 as lat, -74.044444 as lon, 10 as level ) i

select to_hex( h3.geoToH3( radians( i.lat ), radians( i.lon ), i.res ) )
from ( select 40.689167 as lat, -74.044444 as lon, 10 as res ) i


 select 
   st_astext( ST_SetSRID(st_scale( geo, 180.0/pi(), 180.0/pi() ),4326) )
 from 
 (
  select  
   to_hex( ii.h3 ),
   h3IsPentagon( ii.h3 ) as pentagon,
   H3_GET_RESOLUTION(h3) res, 
   _h3ToFaceIjk( h3 ) as fijk,
   h3ToGeoBoundary( ii.h3 ) as geo
  from
  ( select h3.geoToH3( radians( i.lat ), radians( i.lon ), i.res ) as h3 from ( select 40.689167 as lat, -74.044444 as lon, 10 as res ) i ) ii
 ) iii


*/

/*
--- Geoserver rendering views

create or replace view rdr.h3 as 
 select oo.zm, oo.p, oo.h3, st_centroid(oo.geo) as c, oo.geo
 from 
 ( select o1.zm, h3ispentagon( o1.h3) as p, to_hex(o1.h3::bigint) as h3, h3togeoboundarydeg(o1.h3)::geometry(polygon,4326) as geo
    from 
    ( select o.zm, kRing(o.h3, bboxHexRadiusDeg( o.ibox, zoom2resolution(o.zm) )) as h3
      from 
      ( with info as ( select i.ibox, st_centroid(i.ibox) as c, coalesce(bbox2zooml(i.ibox), '-1'::integer) as zm 
                       from ( select st_setsrid(coalesce(gsrv_getreqbbox(current_query()), st_geomfromtext(replace('polyxxx'::text, 'xxx'::text, 'gon'::text) || '((8.65722656250009 50.0923932109386,8.65722656250009 50.1205780979601,8.7011718750001 50.1205780979601,8.7011718750001 50.0923932109386,8.65722656250009 50.0923932109386))'::text)), 4326) as ibox) i
                     )
        select b.zm, b.ibox, geotoh3(radians(st_y(b.c)), radians(st_x(b.c)), zoom2resolution(b.zm)) as h3
        from info b
        where 1 = 1
      ) o
    ) o1
    where 1 = 1
      and o1.h3 != 0
 ) oo
 where 1 = 1;

create or replace view rdr_cal.wv_cluster_delta_h3_f01a as -- select h3, value as total, geo from rdr_cal.wvm_cluster_delta_h3_f01a
select  -- 'POLXXXYGON ((22.32421875 -0.1714300432299857, 22.32421875 22.114475576668156, 45.17578125 22.114475576668156, 45.17578125 -0.1714300432299857, 22.32421875 -0.1714300432299857))'
 to_hex( o.h3 ) as h3,
 o.bqty,
 o.total::bigint AS total,
 o.geo
from 
( 
 with curr_bbox as
 ( select rdr.bbox2zooml( i.ibox ) as zm, i.ibox  
   from 
   ( select st_setsrid( st_envelope(ST_MinimumBoundingCircle(st_envelope(ST_MinimumBoundingCircle( gsrv_getreqbbox(current_query()))))) , 4326 ) AS ibox 
   ) i 
 )
 select i2.h3, sum(w.value) as total, count(1) as bqty, i2.geo
 from rdr_cal.wvm_cluster_delta_h3_f01a w,
 (
  select i1.h3, h3togeoboundarydeg( i1.h3 )::geometry(polygon,4326) as geo
  from
  ( select distinct
     h3ToParent( v.h3, least( greatest( h3.zoom2resolution(b.zm), 4),8) ) AS h3
     from rdr_cal.wvm_cluster_delta_h3_f01a v, curr_bbox b
    where 1 = 1
      and v.geo && b.ibox
      and st_intersects(v.geo, b.ibox)
    -- group by 1 
   ) i1
  ) i2
  where 1 = 1
    and w.geo && i2.geo
    and st_intersects(w.geo, i2.geo)
    and h3IsParent( i2.h3, w.h3 ) 
  group by 1 ,4
) o;

*/
