

// CODE FOR INIT DB


//private static PGSimpleDataSource masterDataSource;
//
//@BeforeAll
//public static void connect() {
//        masterDataSource = new PGSimpleDataSource();
//        masterDataSource.setDatabaseName("postgres");
//        }

//private static Stream<Arguments> databaseArgs() throws SQLException {
//
//        try (Connection conn = masterDataSource.getConnection();
//        PreparedStatement ps = conn.prepareStatement("drop database if exists test")) {
//        ps.execute();
//        }
//
//        try (Connection conn = masterDataSource.getConnection();
//        PreparedStatement ps = conn.prepareStatement("create database test")) {
//        ps.execute();
//        }
//
//        HikariConfig config = new HikariConfig();
//        config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
//        config.addDataSourceProperty("databaseName", "test");
//
//        Persistor persistor = Persistor.create()
//        .connectPostgres(new HikariDataSource(config))
//        .defineDatabaseTypes("dbObjs")
//        .initialize();
//
//        Index dbObjs = persistor.getRecordSet("dbObjs");
//        Record dbObj = dbObjs.get("abc123");
//
//        return Stream.of(
//        Arguments.of(persistor, dbObjs, dbObj)
//        );
//        }

//@ParameterizedTest @MethodSource({"redisArgs", "databaseArgs"})
//    void testRecordTouch(Persistor persistor, Index index, Record record) {
//            try {
//            SetResult result = record.set();
//            assertInsert(result, 0, 0);
//            assertRecord(record, 1, 0, 0);
//            } finally {
//            persistor.close();
//            }
//            }