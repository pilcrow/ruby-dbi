$: << 'lib'
require 'test/unit'
require 'dbi'

class SthTest < Test::Unit::TestCase
    def self.before_setup(sym)
        include sym
    end
end

module SthState::Canceled
    before_setup :execute
end

module MockStHandle
    RECORD_SET =
        [ ['Joe', 19],
          ['Jim', 30],
          ['Bob', 21] ].freeze
    # Borrowed from dbd-pg/ruby-pg
    COLUMN_INFO =
        [ {:name      => 'name',
           :precision => 255,
           :scale     => nil,
           :type_name => "character varying"},
          {:name      => 'age',
           :precision => 4,
           :scale     => nil,
           :type_name => "integer"} ].freeze

    def initialize
        @rows = []
    end

    def execute
        @rows = RECORD_SET.dup
    end

    def cancel
        @rows = []
    end

    def finish
        @rows = nil
    end

    def fetch
        @rows.pop
    end

    def column_info
        COLUMN_INFO.dup
    end
end

module SthBehavior
    module Finished

    end
    module LiveSth
        module Any
            def test_cancel_ok
                assert !@sth.finished

                2.times { assert_nothing_raised { @sth.cancel } }
            end

            def test_execute_ok
                assert !@sth.finished

                2.times {
                    assert_nothing_raised { @sth.execute }
                    assert @sth.fetchable?
                    assert_nothing_raised { @sth.fetch   }
                }
            end
        end
    end

    module Unfetchable
        module Any
            def test_unfetchable
                assert !@sth.fetchable?
            end
        end

    module ReturningEOD
        include Unfetchable::Any

        def test_fetching
            assert !@sth.raise_error, "expected sth.raise_error = false"
            assert !@sth.finished
            assert !@sth.fetchable?

            2.times { assert_nil @sth.fetch }
            2.times { assert_nil @sth.each { |row| } }
            2.times { assert_equal [], @sth.fetch_all }
            2.times { assert_equal [], @sth.fetch_many(5) }
            2.times { assert_equal [], @sth.fetch_array }
            2.times { assert_equal {}, @sth.fetch_hash }
            ['NEXT', 'LAST', 'RELATIVE'].each do |d|
                direction = DBI.const_get('SQL_FETCH_' + d)
                2.times { assert_nil @sth.fetch_scroll(direction, 1) }
            end
        end
    end

    module RaisingError
        include Unfetchable::Any

        def test_fetch_error
            assert @sth.raise_error, "expected sth.raise_error = true"
            assert !@sth.finished
            assert !@sth.fetchable?

            assert_raises(DBI::InterfaceError) {
                assert_nil @sth.fetch
            }
            assert_raises(DBI::InterfaceError) {
                assert_nil @sth.each { |row| }
            }
            assert_raises(DBI::InterfaceError) {
                assert_equal [], @sth.fetch_all
            }
            assert_raises(DBI::InterfaceError) {
                assert_equal [], @sth.fetch_many(5)
            }
            assert_raises(DBI::InterfaceError) {
                assert_equal [], @sth.fetch_array
            }
            assert_raises(DBI::InterfaceError) {
                assert_equal {}, @sth.fetch_hash
            }
            ['NEXT', 'LAST', 'RELATIVE'].each do |d|
                direction = DBI.const_get('SQL_FETCH_' + d)
                assert_raises(DBI::InterfaceError) {
                    assert_nil @sth.fetch_scroll(direction, 1)
                }
            end
        end
    end
end

module ExhaustedSthCommon
    include MockDriver

    def setup
        super
        while @sth.fetch; end
    end

    def test_needless_cancel_okay
        assert !@sth.finished
        assert !@sth.fetchable?

        2.times { assert_nothing_raised { @sth.cancel } }
    end

    def test_not_fetchable
        assert !@sth.finished
        assert !@sth.fetchable?

        2.times { assert !@sth.fetchable? }
    end

    def test_reexecute_okay
        assert !@sth.finished
        assert !@sth.fetchable?

        2.times {
            assert_nothing_raised { @sth.execute }
            assert_nothing_raised { @sth.fetch }
        }
    end

    def test_finish
        assert !@sth.finished
        assert !@sth.fetchable?

        assert_nothing_raised { @sth.finish }
        assert @sth.finished
        assert_raises(DBI::InterfaceError) { @sth.finish }
    end
end

def TestExhaustedSthSilent < Test::Unit::TestCase
    include ExhaustedSthCommon

end

class TestExhaustedSthError < Test::Unit::TestCase
    include ExhaustedSthCommon

    def setup; @sth.raise_error = true end

end

class TC_DBI_StatementHandle < Test::Unit::TestCase
    def setup
        @sth = DBI::StatementHandle.new(MockDriverSth.new, false)
    end

    def test_fetch
        assert_equal false, @sth.fetchable?
        @sth.execute
        assert_equal true, @sth.fetchable?

    end

    def test_unexecuted_fetch_silent
        assert_equal false, @sth.fetchable?

        10.times do
            assert_nil @sth.fetch
        end

        counter = 0
        10.times do
            @sth.fetch { |row| counter = counter + 1 }
        end
        assert_equal 0, counter
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
        end
        assert_equal 0, counter
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

# vim: ts=4 sw=4 et
