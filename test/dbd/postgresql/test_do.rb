# Verify behavior of
#   dbh.do ...

class TestPostgresDo < DBDConfig.testbase(:postgresql)

    # Ordinary behavior:
    #  - last_statement() is correct
    #  - conventional DML queries return rowcount
    def test_update_count
        assert @dbh
        assert @dbh.ping

        assert_equal 3, @dbh.do('UPDATE names SET age = age + 1')
        assert_equal('UPDATE names SET age = age + 1',
                     @dbh.last_statement)

        assert_equal 0, @dbh.do('UPDATE names SET age = age + 1 WHERE 1 = ?', 0)
        assert_equal('UPDATE names SET age = age + 1 WHERE 1 = ?',
                     @dbh.last_statement)
    end

    def test_update_count_nonblocking
        @dbh['pg_async'] = true
        test_update_count
    end
 
    # Driver extension:  we return a meaningful rowcount for SELECT, too.
    def test_select_count
        assert @dbh
        assert @dbh.ping

        assert_equal 6, @dbh.do('SELECT * FROM names UNION ALL SELECT * FROM names')
        assert_equal('SELECT * FROM names UNION ALL SELECT * FROM names',
                     @dbh.last_statement)

        assert_equal 1, @dbh.do('SELECT * FROM names WHERE name = ?', 'Jim')
        assert_equal('SELECT * FROM names WHERE name = ?',
                     @dbh.last_statement)

        assert_equal 0, @dbh.do('SELECT * FROM names WHERE name = ?', 'Blark')
        assert_equal('SELECT * FROM names WHERE name = ?',
                     @dbh.last_statement)
    end

    def test_select_count_nonblocking
        @dbh['pg_async'] = true
        test_select_count
    end
end
