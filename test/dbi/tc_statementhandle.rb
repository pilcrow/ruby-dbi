$: << 'lib'
require 'test/unit'
require 'dbi'

class MockDriverSth
    # def bind_param; end  # not quite yet

    def execute
        @rows = [
            ['Joe', 19],
            ['Jim', 30],
            ['Bob', 21]
        ].dup
    end

    def finish
        @rows = nil
    end

    alias :cancel :finish

    def fetch
        @rows.pop
    end

    def column_info
        return [ {:name      => 'name',
                  :precision => 255,
                  :scale     => nil,
                  :type_name => "character varying"},
                 {:name      => 'age',
                  :precision => 4,
                  :scale     => nil,
                  :type_name => "integer"} ]
    end
end

class TC_DBI_StatementHandle < Test::Unit::TestCase
    def setup
        @sth = DBI::StatementHandle.new(MockDriverSth.new, false)
    end

    def test_unexecuted_fetch_silent
        assert_equal false, @sth.fetchable?

        10.times do
            assert_nil @sth.fetch
        end

        counter = 0
        10.times do
            @sth.fetch { |row| counter = counter + 1 }
            assert_equal 0, counter
        end
    end

    def test_exhausted_fetch_silent
        assert_equal false, @sth.fetchable?

        @sth.execute

        assert_equal true, @sth.fetchable?

        assert_nothing_raised do
            @sth.fetch_all
        end

        assert_equal false, @sth.fetchable?

        10.times do
            assert_nil @sth.fetch
        end

        assert_equal false, @sth.fetchable?

        counter = 0
        10.times do
            @sth.fetch { |row| counter = counter + 1 }
            assert_equal 0, counter
        end
    end

    def test_unexecuted_fetch_error
        @sth.raise_error = true

        assert_equal false, @sth.fetchable?

        assert_raises(DBI::InterfaceError) do
            @sth.fetch
        end

        counter = 0
        assert_raises(DBI::InterfaceError) do
            @sth.fetch { |row| counter = counter + 1 }
        end
        assert_equal 0, counter
    end
end
