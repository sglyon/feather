using Missings, DataFrames, Feather, CategoricalArrays

_path(x) = joinpath(@__DIR__, "test_data", x)

function main()
    test2 = DataFrame(
        x_Bool=Bool[1, 1, 0, 1, 1, 1, 0, 0, 0, 1],
        x_Int8=map(Int8, 1:10),
        x_Int32=map(Int32, 2:11),
        x_Int16=map(Int16, 3:12),
        x_Int64=map(Int64, 4:13),
        x_UInt8=map(UInt8, 5:14),
        x_UInt16=map(UInt16, 6:15),
        x_UInt32=map(UInt32, 7:16),
        x_UInt64=map(UInt64, 8:17),
        x_Float32=map(Float32, 9:18),
        x_Float64=map(Float64, 10:19),
        x_String=[*(('A':'Z')[x]...) for x in [((i-1)*2 + 1):2i for i in 1:10]]
    )
    Feather.write(_path("test2.feather"))

    nms = names(test2)
    for col in 1:length(nms)
        println("""want_$(col-1) := $(repr(test2[:, nms[col]]))
        col$(col-1) := src.Columns[$(col-1)].(*$(string(nms[col])[3:end])Column)
        vals$(col-1), nulls$(col-1) := col$(col-1).Values()
        """)
    end

    x = convert(Array{Union{Int,Missing}}, 1:10)
    x[3] = missing
    x[9] = missing

    x_string = convert(Array{Union{String,Missing}}, test2[:, :x_String])
    x_string[3] = missing
    x_string[9] = missing

    test2missing = DataFrame(
        x_Bool=Union{Bool,Missing}[1, 1, missing, 1, 1, 1, 0, 0, missing, 1],
        x_Int8=convert(Array{Union{Int8,Missing}}, x.+0),
        x_Int32=convert(Array{Union{Int32,Missing}}, x.+1),
        x_Int16=convert(Array{Union{Int16,Missing}}, x.+2),
        x_Int64=convert(Array{Union{Int64,Missing}}, x.+3),
        x_UInt8=convert(Array{Union{UInt8,Missing}}, x.+4),
        x_UInt16=convert(Array{Union{UInt16,Missing}}, x.+5),
        x_UInt32=convert(Array{Union{UInt32,Missing}}, x.+6),
        x_UInt64=convert(Array{Union{UInt64,Missing}}, x.+7),
        x_Float32=convert(Array{Union{Float32,Missing}}, x.+8),
        x_Float64=convert(Array{Union{Float64,Missing}}, x.+9),
        x_String=x_string
    )
    Feather.write(_path("test2missing.feather"))

    cats = DataFrame(
        cat_Float32=CategoricalArray(Union{Float32,Missing}[1, 2, 3, 1, 1, 2, 2, 3, 3, missing]),
        cat_String=CategoricalArray(["a", "b", "c", "a", "a", "b", "b", "c", "c", "b"])
    )
    Feather.write(_path("cats.feather"))

    mybytes = read(_path("cats.feather"))
    open(_path("missing_fea1_start.feather"), "w") do f
        write(f, mybytes[5:end])
    end

    open(_path("missing_fea1_end.feather"), "w") do f
        write(f, mybytes[1:end-4])
    end

end

main()
