/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
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
 * #L%
 */
package com.amazonaws.connectors.athena.jdbc.manager;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility abstracts Jdbc to Arrow type conversions.
 */
public final class JdbcArrowTypeConverter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetadataHandler.class);

    private JdbcArrowTypeConverter() {}

    /**
     * Coverts Jdbc data type to Arrow data type.
     *
     * @param jdbcType Jdbc integer type. See {@link java.sql.Types}.
     * @param precision Decimal precision.
     * @param scale Decimal scale.
     * @return Arrow type. See {@link ArrowType}.
     */
    public static ArrowType toArrowType(final int jdbcType, final int precision, final int scale)
    {
        ArrowType arrowType = JdbcToArrowUtils.getArrowTypeForJdbcField(
                new JdbcFieldInfo(jdbcType, precision, scale),
                null);

        if (arrowType instanceof ArrowType.Date) {
            // Convert from DateMilli to DateDay
            return new ArrowType.Date(DateUnit.DAY);
        }
        else if (arrowType instanceof ArrowType.Timestamp) {
            // Convert from Timestamp to DateMilli
            return new ArrowType.Date(DateUnit.MILLISECOND);
        }
        else if (arrowType instanceof ArrowType.Decimal) {
            // en liten bug.., postgres numeric går langt ut over hva en 128 bits verdi kan takle som en Arrow datatype
            // vi gjør en grov antagelse her, hvis det ikke holder så kan man legge på precision og scale på datatypen i postgres
            // https://github.com/awslabs/aws-athena-query-federation/issues/360

            var d = (ArrowType.Decimal)arrowType;
            if (d.getPrecision() > 31) {
                return d.getScale() == 0 ? new ArrowType.Decimal(31, 15, 128) :
                    new ArrowType.Decimal(31, scale, 128);
            };            
        }


        return arrowType;
    }
}
