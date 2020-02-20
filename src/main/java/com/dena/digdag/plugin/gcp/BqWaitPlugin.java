/*
* SPDX-FileCopyrightText: 2020 DeNA Co., Ltd.
* SPDX-License-Identifier: Apache-2.0
*
* Copyright (c) 2020 DeNA Co., Ltd.
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
package com.dena.digdag.plugin.gcp;

import java.util.Collections;
import java.util.List;

import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;

public class BqWaitPlugin implements Plugin {

    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return BqWaitOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    private static class BqWaitOperatorProvider implements OperatorProvider {

        @Override
        public List<OperatorFactory> get() {
            return Collections.singletonList(new BqWaitOperatorFactory());
        }

    }
}
