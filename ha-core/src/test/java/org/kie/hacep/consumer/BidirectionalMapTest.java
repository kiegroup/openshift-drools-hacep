/*
 * Copyright 2019 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kie.hacep.consumer;

import static org.junit.Assert.*;
import org.junit.Test;

public class BidirectionalMapTest {

    @Test
    public void test(){
        BidirectionalMap map = new BidirectionalMap();
        map.put("one", 1);
        Object res = map.getKey(1);
        assertNotNull(res);
        assertTrue(res.equals("one"));
        assertNotNull(map.removeValue(1));
        map.put("one", 2);
        assertNotNull(map.remove("one"));
    }
}
