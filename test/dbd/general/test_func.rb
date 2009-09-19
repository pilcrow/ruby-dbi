@class = Class.new(DBDConfig.testbase(DBDConfig.current_dbtype)) do

    # Add a testing accessor var to the underlying @handle
    def setup
        super
        class << @dbh.handle
            attr_accessor :__var
        end
    end

    def test_func_arg_err_nullary
        class << @dbh.handle
            def __dbd_general_test_nullary; self.__var = "ok" end
        end

        assert_nil @dbh.handle.__var
        assert_nothing_raised do
          @dbh.func :dbd_general_test_nullary
        end
        assert_equal "ok", @dbh.handle.__var

        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_nullary, "dummy")
        end

        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_nullary, "dummy", "dummy")
        end
    end

    def test_func_arg_err_nullary_plus
        class << @dbh.handle
            def __dbd_general_test_nullary_plus(*optional); self.__var = "ok" end
        end

        assert_nil @dbh.handle.__var
        assert_nothing_raised do
            @dbh.func(:dbd_general_test_nullary_plus)
            @dbh.func(:dbd_general_test_nullary_plus, "dummy")
            @dbh.func(:dbd_general_test_nullary_plus, "dummy", "dummy")
        end
        assert_equal "ok", @dbh.handle.__var
    end

    def test_func_arg_err_unary
        class << @dbh.handle
            def __dbd_general_test_unary(a); self.__var = a end
        end

        assert_nil @dbh.handle.__var
        assert_nothing_raised do
          @dbh.func(:dbd_general_test_unary, "ok")
        end
        assert_equal "ok", @dbh.handle.__var

        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_unary)
        end

        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_unary, "dummy", "dummy")
        end
    end

    def test_func_arg_err_unary_plus
        class << @dbh.handle
            def __dbd_general_test_unary_plus(a, *optional); self.__var = a    end
        end

        assert_nil @dbh.handle.__var
        assert_nothing_raised do
          @dbh.func(:dbd_general_test_unary_plus, "ok", "ignored", 1, 2, 3)
        end
        assert_equal "ok", @dbh.handle.__var

        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_unary_plus)
        end
    end

    def test_func_nometh_err
        assert_raises(DBI::InterfaceError) do
            @dbh.func(:dbd_general_test_a_method_quite_unlikely_to_exist)
        end
    end

    def test_func_block
        class << @dbh.handle
            def __dbd_general_test_block(*a); yield end
        end

        assert_nothing_raised do
            a = 0
            @dbh.func(:dbd_general_test_block) do
                a = 10
            end
            assert_equal(10, a)
        end
    end

    def test_implementor_argument_error
        class << @dbh.handle
            def __dbd_general_test_argument_error
                raise ArgumentError, "contrived"
            end
        end
        assert_raises(ArgumentError) do
            @dbh.func(:dbd_general_test_argument_error)
        end
    end

    def test_implementor_no_method
        class << @dbh.handle
            def __dbd_general_test_no_method
                raise NoMethodError, "contrived"
            end
        end
        assert_raises(NoMethodError) do
            @dbh.func(:dbd_general_test_no_method)
        end
    end

    def test_implementor_local_jump_error
        class << @dbh.handle
            def __dbd_general_ad_hoc_requires_block
                yield
            end
            def __dbd_general_test_lje
                __dbd_general_ad_hoc_requires_block
            end
        end

        assert_raises(LocalJumpError) do
            @dbh.func(:dbd_general_test_lje)
        end
    end
end
