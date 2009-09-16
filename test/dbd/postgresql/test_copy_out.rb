# Verify behavior of:
#
#   dbh.func :get_copy_data
#

class TestPostgresCopyOut < DBDConfig.testbase(:postgresql)

    SORTED_NAMES_EXPORT = [ "Bob\t21\n", "Jim\t30\n", "Joe\t19\n" ]

    def test_no_COPY_in_progress_get
        assert_raises(DBI::DatabaseError) do
            @dbh.func :get_copy_data
        end
    end

    def test_no_COPY_in_progress_get_nonblocking
        @dbh['pg_async'] = true

        assert_raises(DBI::DatabaseError) do
            @dbh.func :get_copy_data
        end
    end

    def test_copy_stdout_blocking
        @dbh['pg_async'] = false

        copied = do_copy_to_stdout(@dbh)
        copied.sort!

        assert_equal SORTED_NAMES_EXPORT, copied
    end

    def test_copy_stdout_nonblocking
        @dbh['pg_async'] = true

        fixup_broken_async_exec( @dbh )  # XXX:  blech

        copied = do_copy_to_stdout(@dbh)
        copied.sort!

        assert_equal SORTED_NAMES_EXPORT, copied
    end

    private

    def do_copy_to_stdout(dbh)
        dbh.ping

        copied_out = []
        assert_nothing_raised do
            dbh.do("COPY names TO STDOUT")
            3.times do
                copied_out << dbh.func(:get_copy_data)
            end
            assert_nil dbh.func(:get_copy_data)
        end

        copied_out  
    end

    def fixup_broken_async_exec(dbh)

      # ruby-pg-0.8.0's async_exec (actually, get_last_result) loops
      # infinitely on COPY_OUT.  Here we reimplement that method's logic
      # but break loop on COPY states

      raise ArgumentError.new("Expected DBI::DatabaseHandle") unless dbh.is_a?(DBI::DatabaseHandle)

      pg_conn = [:@handle, :@connection].inject(dbh) do |obj, iv|
        obj.instance_variable_get(iv)
      end

      raise "Unable to fetch underlying PGconn of the given DBI::DatabaseHandle" unless pg_conn.is_a?(PGconn)
 
      class << pg_conn
        PGResStates = {
          :ok          => [  PGresult::PGRES_TUPLES_OK,
                             PGresult::PGRES_EMPTY_QUERY,
                             PGresult::PGRES_COMMAND_OK      ],

          :in_progress => [  PGresult::PGRES_COPY_OUT,
                             PGresult::PGRES_COPY_IN         ],

          :error       => [  PGresult::PGRES_BAD_RESPONSE,
                             PGresult::PGRES_FATAL_ERROR,
                             PGresult::PGRES_NONFATAL_ERROR  ],
                             # XXX: IMHO, EMPTY_QUERY is :error -mjp
        }

        # Fetch the last of N PGresult objects, stopping where appropriate
        #
        # Compare ruby-pg-0.8.0 pg.c:get_last_result, which does not break
        # out of the get_result (PQgetResult) loop on COPY_OUT and COPY_IN
        #
        def get_last_result
          pg_res = nil
          errmsg = nil

          catch :all_done do
            while true
              cur = get_result
              throw :all_done if cur.nil?
              pg_res = cur

              case pg_res.result_status
              when *PGResStates[:ok]
                # noop
              when *PGResStates[:in_progress]
                throw :all_done
              when *PGResStates[:error]
                errmsg = pg_res.result_error_message
                throw :all_done
              else
                errmsg = "internal error : unknown result status."
                throw :all_done
              end
            end
          end

          if errmsg
            exc = PGconn::PGError.new(errmsg)
            exc.instance_variable_set(:@connection, self)
            exc.instance_variable_set(:@result, pg_res)
            raise exc
          end

          return pg_res
        end

        # Execute the given query, allowing green-threaded ruby to continue
        # while the query executes on the server.
        #
        # See ruby-pg-0.8.0 pg.c:async_exec
        #
        def async_exec(sql, params = nil, res_fmt = nil)
          send_query(sql, params, res_fmt)
          block
          get_last_result
        end #-- def async_exec
      end #-- class << pg_conn
    end

end
