object MyTimer {
    def timeit[R](block: => R): R = {
        val t1 = System.nanoTime()
        val result = block    // call-by-name
        val t2 = System.nanoTime()
        println("### timeit() : Elapsed time:  %,d  ms".format( (t2 - t1) /1000000 ) )
        result
    }
}