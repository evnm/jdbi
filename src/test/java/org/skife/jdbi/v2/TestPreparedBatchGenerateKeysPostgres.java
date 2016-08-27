/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.jdbi.v2;

import org.junit.*;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerColumnMapper;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestPreparedBatchGenerateKeysPostgres {

    private Handle h;

    @BeforeClass
    public static void isPostgresInstalled() {
        assumeTrue(Boolean.parseBoolean(System.getenv("TRAVIS")));
    }

    @Before
    public void setUp() throws Exception {
        h = new DBI("jdbc:postgresql:jdbi_test", "postgres", "").open();
        h.execute("create table something (id serial, name varchar(50), create_time timestamp default now())");
        h.execute("create function insert_func(varchar(50)) returns void as "
                 + "'insert into something (name) values ($1);' LANGUAGE SQL");
    }

    @After
    public void tearDown() throws Exception {
        h.execute("drop table something");
        h.execute("drop function insert_func(varchar)");
        h.close();
    }

    @Test
    public void testBatchInsertWithKeyGenerationAndExplicitColumnNames() {
        PreparedBatch batch = h.prepareBatch("insert into something (name) values (?) ");
        batch.add("Brian");
        batch.add("Thom");

        List<Integer> ids = batch.executeAndGenerateKeys(IntegerColumnMapper.WRAPPER, "id").list();
        assertEquals(Arrays.asList(1, 2), ids);

        List<Something> somethings = h.createQuery("select id, name from something")
                .map(Something.class)
                .list();
        assertEquals(Arrays.asList(new Something(1, "Brian"), new Something(2, "Thom")), somethings);
    }

    @Test
    public void testBatchInsertWithKeyGenerationAndExplicitSeveralColumnNames() {
        PreparedBatch batch = h.prepareBatch("insert into something (name) values (?) ");
        batch.add("Brian");
        batch.add("Thom");

        List<IdCreateTime> ids = batch.executeAndGenerateKeys(new ResultSetMapper<IdCreateTime>() {
            @Override
            public IdCreateTime map(int index, ResultSet r, StatementContext ctx) throws SQLException {
                return new IdCreateTime(r.getInt("id"), r.getDate("create_time"));
            }
        }, "id", "create_time").list();

        assertEquals(ids.size(), 2);
        assertTrue(ids.get(0).id == 1);
        assertTrue(ids.get(1).id == 2);
        assertNotNull(ids.get(0).createTime);
        assertNotNull(ids.get(1).createTime);
    }

    @Test
    public void testBatchVoidFunctionInvocation() throws SQLException {
        PreparedBatch batch = h.prepareBatch("select insert_func(?)");
        batch.add("Brian");
        batch.add("Thom");

        try {
          batch.execute();
        } catch (UnableToExecuteStatementException err) {
          throw ((BatchUpdateException) err.getCause()).getNextException();
        }

        List<Something> somethings = h.createQuery("select id, name from something")
                .map(Something.class)
                .list();
        assertEquals(Arrays.asList(new Something(1, "Brian"), new Something(2, "Thom")), somethings);
    }

    private static class IdCreateTime {

        final Integer id;
        final Date createTime;

        public IdCreateTime(Integer id, Date createTime) {
            this.id = id;
            this.createTime = createTime;
        }
    }
}
