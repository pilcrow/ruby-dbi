$: << 'lib'
require 'test/unit'
require 'dbi'

module MandatorySthTests
  @@oops = ::NotImplementedError.new("Developer forgot to write test!")

  def test_fetchable;   raise @@oops end
  def test_finished;    raise @@oops end
  def test_fetch;       raise @@oops end
  def test_cancel;      raise @@oops end
  def test_execute;     raise @@oops end
  def test_finish;      raise @@oops end
  def test_bind_param;  raise @@oops end # XXX - DBI should enforce count...
  def test_column_info; raise @@oops end

  private
  def me
    self.class.name.sub(/^Test/, '')
  end
end

module SthBehaviorUnexecuted
  def test_fetchable
    assert !@sth.fetchable?, "#{me} is not fetchable"
  end
  def test_finished
    assert !@sth.finished?, "#{me} is not #finish()d"
  end
  def test_cancel
    assert_nothing_raised("#{me} may be harmlessly #cancel()d") do
      @sth.cancel
    end
  end
  def test_finish
    assert_nothing_raised("#{me} may be #finish()d") do
      @sth.finish
    end
    assert @sth.finished
  end
  def test_execute
    2.times {
      assert_nothing_raised("#{me} may be executed") do
        @sth.execute
      end
    }
  end
  def test_column_info
    assert_raises(DBI::InterfaceError) do
    end
  end
end

class TestSthInitialized; end
class TestSthCanceled; end
class TestSthExecuted; end
class TestSthExecutedUnbound; end
class TestSthPartiallyFetched; end
class TestSthExhaustedSilent; end
class TestSthExhaustedError; end
class TestSthExecuted; end
class TestSthFinished; end

class TestSthUnexecuted < Test::Unit::TestCase
  include MandatorySthTests

  def test_finished
    assert !@sth.finished
  end

  def test_bind_param
    assert_raises(DBI::InterfaceError) do
      @sth.bind_param(1, "Billy-Mack")
    end
  end

  def test
end

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
