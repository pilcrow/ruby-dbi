# Verify behavior of:
#
#   dbh.func :put_copy_data
#   dbh.func :put_copy_end
#

class TestPostgresCopyIn < DBDConfig.testbase(:postgresql)
    # XXX - test_no_COPY_in_progress methods should all check
    #       for DBI::ProgrammingError, but until we can
    #       inspect COPY results or SQLSTATE reliably, dbd-pg
    #       won't be able to distinguish programming errors
    #       from, e.g., db connection resets.
    #
    def test_no_COPY_in_progress_put
      assert_raises(DBI::DatabaseError) do
        @dbh.func :put_copy_data, "Bob\t21\n"
      end
    end

    def test_no_COPY_in_progress_end
      assert_raises(DBI::DatabaseError) do
        @dbh.func :put_copy_end
      end

      assert_raises(DBI::DatabaseError) do
        @dbh.func :put_copy_end, "User-supplied error"
      end
    end

    def test_copy_stdin
        assert @dbh
        assert @dbh.ping

        assert_nothing_raised do
            @dbh.do('COPY names FROM STDIN')
            [ "Foo\t99\n", "Bar\t89\n", "Baz\t79\n" ].each do |ln|
                @dbh.func :put_copy_data, ln
            end
            @dbh.func :put_copy_end
        end

        assert_equal [ ['Bar'], ['Baz'], ['Bob'], ['Foo'], ['Jim'], ['Joe'] ],
                     @dbh.select_all("SELECT name FROM names ORDER BY name")
        assert_equal [ [19], [21], [30], [79], [89], [99] ],
                     @dbh.select_all("SELECT age FROM names ORDER BY age")
    end

    def test_copy_stdin_terminate
        assert @dbh
        assert @dbh.ping

        assert_nothing_raised do
            @dbh.do('COPY names FROM STDIN')
            [ "Foo\t99\n", "Bar\t89\n", "Baz\t79\n" ].each do |ln|
                @dbh.func :put_copy_data, ln
            end

            # user termination of COPY raises no error
            @dbh.func :put_copy_end, "Aaaaaaaaaaaaaaaaaaaaaaaargh"
        end

        assert_equal [ ['Bob'], ['Jim'], ['Joe'] ],
                     @dbh.select_all("SELECT name FROM names ORDER BY name")
        assert_equal [ [19], [21], [30] ],
                     @dbh.select_all("SELECT age FROM names ORDER BY age")
    end

end
