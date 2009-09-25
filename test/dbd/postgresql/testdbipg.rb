require 'dbd/Pg'

######################################################################
# Test the PostgreSql DBD driver.  This test exercises options
# difficult to test through the standard DBI interface.
#
class TestDbdPostgres < DBDConfig.testbase(:postgresql)

    # FIXME this is a feature that should be there, but currently isn't.
#   def test_connect
#     dbd = get_dbd
#     assert_not_nil dbd.connection
#     assert_equal 'localhost', dbd.connection.host
#     assert_equal 'erikh', dbd.connection.user
#     assert_equal 'rubytest', dbd.connection.db
#     assert_equal 5432, dbd.connection.port
#   ensure
#      dbd.disconnect if dbd
#   end

    # this monkeypatch is used for the following test... NEVER integrate this into DBI proper.
    class DBI::StatementHandle < DBI::Handle
        def internal_name
            @handle.instance_variable_get(:@plan_name)
        end
    end

    def test_database_name
        assert_nothing_raised do
            assert_equal DBDConfig.get_config[dbtype]['dbname'], @dbh.database_name
        end
    end

    def test_enum_type
        assert_nothing_raised do
            assert(@dbh.convert_types)
            @sth = @dbh.prepare("insert into enum_type_test values (?)")
            @sth.execute("one")
            @sth.finish

            @sth = @dbh.prepare("select foo from enum_type_test")
            @sth.execute
            assert_equal(@sth.fetch, ['one'])
            @sth.finish
        end
    end

    def test_statement_finish_deallocates_pg_plan
        assert_nothing_raised do
            @sth = @dbh.prepare("select * from names")
            @sth.execute
            sth_internal_name = @sth.internal_name
            assert(sth_internal_name)
            assert(!sth_internal_name.empty?)
            assert_raises(DBI::ProgrammingError) do
                # Should collide with extant plan name
                @dbh.do("PREPARE \"#{sth_internal_name}\" AS SELECT 1")
            end
            @sth.finish

            # Now no collision
            assert_nothing_raised do
                @dbh.do("PREPARE \"#{sth_internal_name}\" AS SELECT 1")
            end
        end
    end

    def test_prepare_block_deallocates_pg_plan
        assert_nothing_raised do
            sth_internal_name = nil

            @dbh.prepare("select * from names") do |sth|
                sth.execute
                sth_internal_name = sth.internal_name
                assert(sth_internal_name)
                assert(!sth_internal_name.empty?)
                assert_raises(DBI::ProgrammingError) do
                    # Should collide with extant plan name
                    @dbh.do("PREPARE \"#{sth_internal_name}\" AS SELECT 1")
                end
            end

            # if finish did not remove the pg plan name, the following
            # PREPARE will fail with a low-level name collision.
            assert_nothing_raised do
                @dbh.do("PREPARE \"#{sth_internal_name}\" AS SELECT 1")
            end
        end
    end

    def test_pg_native_binding_deprecated
        save_warn = $-w

        $-w = nil  # same as -W0

        # 'pg_native_binding' is faux-supported, hardcoded to true
        # and accepting any true assignment
        assert_nothing_raised do
            assert(true == @dbh['pg_native_binding'])
            @dbh['pg_native_binding'] = 'Yes, please'
            assert(true == @dbh['pg_native_binding'])
        end

        assert_raises(DBI::InterfaceError) do
            @dbh['pg_native_binding'] = false
        end

        assert_raises(DBI::InterfaceError) do
            @dbh['pg_native_binding'] = nil
        end
    ensure
        $-w = save_warn
    end

    def test_function_multiple_return_values
        @sth = @dbh.prepare("SELECT age, select_subproperty(age, NULL), select_subproperty(age, 1) FROM names WHERE age = 19")
        @sth.execute
        assert_equal([[19, nil, 19]], @sth.fetch_all)
        @sth.finish
    end

    def test_columns
        assert_equal(
            [
                {
                        :name =>"age",
                        :default =>nil,
                        :primary =>nil,
                        :scale =>nil,
                        :sql_type =>4,
                        :nullable =>true,
                        :indexed =>false,
                        :precision =>4,
                        :type_name =>"integer",
                        :unique =>nil,
                        :array_of_type =>nil
                },
                {
                        :name =>"name",
                        :default =>nil,
                        :primary =>nil,
                        :scale =>nil,
                        :sql_type =>12,
                        :nullable =>true,
                        :indexed =>false,
                        :precision =>255,
                        :type_name =>"character varying",
                        :unique =>nil,
                        :array_of_type =>nil
                }
        ], @dbh.columns("names").sort_by { |x| x["name"] })

        assert_equal(2, @dbh.columns("names").size) # make sure this works before the search path change

        assert_equal(0, @dbh.columns("tbl").size) # tbl doesn't exist in public

        @dbh.do('SET search_path TO schema1,schema2,"$user",public')

        assert_equal(1, @dbh.columns('tbl').size);
        assert_equal(
            [
                {
                    :name =>"foo",
                    :default =>nil,
                    :primary =>nil,
                    :scale =>nil,
                    :sql_type =>4,
                    :nullable =>true,
                    :indexed =>false,
                    :precision =>4,
                    :type_name =>"integer",
                    :unique =>nil,
                    :array_of_type =>nil
        
                }
            ], 
            @dbh.columns('tbl')
        )
                                
    end

  def test_connect_errors
    dbd = nil
    ex = assert_raises(DBI::OperationalError) {
      dbd = DBI::DBD::Pg::Database.new('rubytest:1234', 'jim', nil, {})
    }
    ex = assert_raises(DBI::OperationalError) {
      dbd = DBI::DBD::Pg::Database.new('bad_db_name', 'jim', nil, {})
    }

    # this corresponds to the test_parse_url_expected_errors test in tc_dbi.rb
    assert_raises(DBI::InterfaceError) do
        DBI.connect("dbi:Pg").disconnect
    end

  ensure
    dbd.disconnect if dbd
  end

  def skip_test_type_map
    dbd = get_dbd
    def dbd.type_map
      @type_map
    end
    assert dbd.type_map
    assert_equal 21, dbd.convert("21", 23)
    assert_equal "21", dbd.convert("21", 1043)
    assert_equal 21.5, dbd.convert("21.5", 701)
  end

  def test_simple_command
    dbd = get_dbd
    res = dbd.do("INSERT INTO names (name, age) VALUES('Dan', 16)")
    assert_equal 1, res
    
    @sth = get_dbi.prepare("SELECT name FROM names WHERE age=16")
    @sth.execute
    assert @sth.fetchable?
    # XXX FIXME This is a bug in the DBD. #rows should equal 1 for select statements.
    assert_equal 0, @sth.rows
  ensure
    dbd.do("DELETE FROM names WHERE age < 20")
    dbd.disconnect if dbd
  end

  def test_bad_command
    dbd = get_dbd
    assert_raises(DBI::ProgrammingError) {
      dbd.do("INSERT INTO bad_table (name, age) VALUES('Dave', 12)")
    }
  ensure
    dbd.disconnect if dbd
  end

  def test_query_single
    dbd = get_dbi
    res = dbd.prepare("SELECT name, age FROM names WHERE age=21;")
    assert res
    res.execute
    fields = res.column_info
    assert_equal 2, fields.length
    assert_equal 'name', fields[0]['name']
    assert_equal 'varchar', fields[0]['type_name']
    assert_equal 'age', fields[1]['name']
    assert_equal 'int4', fields[1]['type_name']

    row = res.fetch

    assert_equal 'Bob', row[0]
    assert_equal 21, row[1]

    row = res.fetch
    assert_nil row

    res.finish
  ensure
    dbd.disconnect if dbd
  end

  def test_query_multi
    dbd = get_dbd
    res = dbd.prepare("SELECT name, age FROM names WHERE age > 20;")

    expected_list = ['Jim', 'Bob', 'Charlie']
    res.execute
    while row=res.fetch
      expected = expected_list.shift
      assert_equal expected, row[0]
    end

    res.finish
  ensure
    dbd.disconnect if dbd
  end

  def test_tables_call
      # per bug #1082, views do not show up in tables listing.
      assert get_dbi.tables.include?("view_names")
  end
  
  def get_dbi
      config = DBDConfig.get_config
      DBI.connect("dbi:Pg:#{config['postgresql']['dbname']}", config['postgresql']['username'], config['postgresql']['password'])
  end

  def get_dbd
      config = DBDConfig.get_config['postgresql']
      result = DBI::DBD::Pg::Database.new(config['dbname'], config['username'], config['password'], {})
      result['AutoCommit'] = true
      result
  end
end

# --------------------------------------------------------------------
