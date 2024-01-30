package ceng.ceng351.cengvacdb;

import java.sql.*;
import java.util.ArrayList;

public class CENGVACDB implements ICENGVACDB {

    private static String user = "e2380533";
    private static String password = "y-+QLcRO?@rx";
    private static String host = "144.122.71.121";
    private static String database = "db2380533";
    private static int port = 8080;

    private static Connection connection = null;

    @java.lang.Override
    public void initialize() {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    @java.lang.Override
    public int createTables() {


        int numberofTablesInserted = 0;

        // User (userID:int, userName:varchar(30), age:int, address:varchar(150), password:varchar(30),
        //status:varchar(15))
        String queryCreateUserTable = "create table if not exists user (" +
                "userID int ," +
                "userName varchar(30) ," +
                "age int ," +
                "address varchar(150) ," +
                "password varchar(30) ," +
                "status varchar(15) ," +
                "primary key (userID))";

        // Vaccine (code:int, vaccinename:varchar(30), type:varchar(30))
        String queryCreateVaccineTable = "create table if not exists vaccine (" +
                "code int ," +
                "vaccinename varchar(30) ," +
                "type varchar(30) ," +
                "primary key (code))" ;

        // Vaccination (code:int, userID:int, dose:int, vacdate:date) References Vaccine (code), User
        //(userID)
        String queryCreateVaccinationTable = "create table if not exists vaccination (" +
                "code int," +
                "userID int," +
                "dose int," +
                "vacdate date," +
                "primary key (code, userID, dose)," +
                "foreign key (userID) references user(userID) on delete cascade on update cascade," +
                "foreign key (code) references vaccine(code) on delete cascade on update cascade)";

        //AllergicSideEffect (effectcode:int, effectname:varchar(50))
        String queryCreateAllergicSideEffectTable = "create table if not exists allergicsideeffect (" +
                "effectcode int," +
                "effectname varchar(50)," +
                "primary key (effectcode))";

        //Seen (effectcode:int, code:int, userID:int, date:date, degree:varchar(30)) References Allergic-
        //SideEffect (effectcode), Vaccination (code), User (userID)
        String queryCreateSeenTable = "create table if not exists seen (" +
                "effectcode int," +
                "code int," +
                "userID int," +
                "date date," +
                "degree varchar(30)," +
                "primary key (effectcode, code, userID)," +
                "foreign key (effectcode) references allergicsideeffect(effectcode) on delete cascade on update cascade," +
                "foreign key (code) references vaccination(code) on delete cascade on update cascade," +
                "foreign key (userID) references user(userID) on delete cascade on update cascade)";


        try {
            Statement statement = this.connection.createStatement();

            statement.executeUpdate(queryCreateUserTable);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateVaccineTable);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateVaccinationTable);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateAllergicSideEffectTable);
            numberofTablesInserted++;

            statement.executeUpdate(queryCreateSeenTable);
            numberofTablesInserted++;

            statement.close();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofTablesInserted;
    }

    @java.lang.Override
    public int dropTables() {


        int numberofTablesDropped = 0;

        String queryDropUserTable = "drop table if exists user";

        String queryDropVaccineTable = "drop table if exists vaccine";

        String queryDropVaccinationTable = "drop table if exists vaccination";

        String queryDropAllergicSideEffectTable = "drop table if exists allergicsideeffect";

        String queryDropSeenTable = "drop table if exists seen";


        try {
            Statement statement = this.connection.createStatement();

            statement.executeUpdate(queryDropSeenTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropVaccinationTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropVaccineTable);
            numberofTablesDropped++;

            statement.executeUpdate(queryDropAllergicSideEffectTable);
            numberofTablesDropped++;



            statement.executeUpdate(queryDropUserTable);
            numberofTablesDropped++;

            //close
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return numberofTablesDropped;
    }

    @java.lang.Override
    public int insertUser(User[] users) {


        int numberofRowsInserted = 0;

        for (int i = 0; i < users.length; i++)
        {
            try {
                User use = users[i];

                PreparedStatement stmt=this.connection.prepareStatement("insert into user values(?,?,?,?,?,?)");
                stmt.setInt(1,use.getUserID());
                stmt.setString(2,use.getUserName());
                stmt.setInt(3,use.getAge());
                stmt.setString(4,use.getAddress());
                stmt.setString(5,use.getPassword());
                stmt.setString(6,use.getStatus());

                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @java.lang.Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {


        int numberofRowsInserted = 0;

        for (int i = 0; i < sideEffects.length; i++)
        {
            try {
                AllergicSideEffect ase = sideEffects[i];

                PreparedStatement stmt=this.connection.prepareStatement("insert into allergicsideeffect values(?,?)");
                stmt.setInt(1,ase.getEffectCode());
                stmt.setString(2,ase.getEffectName());

                stmt.executeUpdate();

                //Close
                stmt.close();
                numberofRowsInserted++;

            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;
    }

    @java.lang.Override
    public int insertVaccine(Vaccine[] vaccines) {

        int numberofRowsInserted =  0;

        for(int i=0; i< vaccines.length; i++){
            try{
                Vaccine vac =vaccines[i];
                PreparedStatement stmt = this.connection.prepareStatement("insert into vaccine values(?,?,?)");
                stmt.setInt(1,vac.getCode());
                stmt.setString(2,vac.getVaccineName());
                stmt.setString(3,vac.getType());

                stmt.executeUpdate();
                stmt.close();
                numberofRowsInserted++;

            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberofRowsInserted;
    }

    @java.lang.Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int numberofRowsInserted = 0;

        for(int i=0; i< vaccinations.length; i++){
            try{
                Vaccination vcc = vaccinations[i];
                PreparedStatement stmt = this.connection.prepareStatement("insert into vaccination values(?,?,?,?)");
                stmt.setInt(1,vcc.getCode());
                stmt.setInt(2,vcc.getUserID());
                stmt.setInt(3,vcc.getDose());
                stmt.setString(4,vcc.getVacdate());


                stmt.executeUpdate();
                stmt.close();
                numberofRowsInserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;

    }

    @java.lang.Override
    public int insertSeen(Seen[] seens) {
        int numberofRowsInserted = 0;

        for(int i=0; i<seens.length; i++){
            try{
                Seen see = seens[i];
                String s = see.getUserID();
                int b=Integer.parseInt(s);
                PreparedStatement stmt = this.connection.prepareStatement("insert into seen values(?,?,?,?,?)");
                stmt.setInt(1,see.getEffectcode());
                stmt.setInt(2,see.getCode());
                stmt.setInt(3,b);
                stmt.setString(4,see.getDate());
                stmt.setString(5,see.getDegree());
                

                stmt.executeUpdate();
                stmt.close();
                numberofRowsInserted++;
            }

            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return numberofRowsInserted;

    }

    @java.lang.Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {

        ResultSet rs;
        ArrayList<Vaccine> reslist = new ArrayList<>();

        String query = "SELECT DISTINCT V.code , V.vaccinename , V.type  \n" +
                "FROM vaccine V \n" +
                "WHERE V.code NOT IN (SELECT V1.code" +
                "\tFROM vaccination V1 , vaccine V2\n" +
                "\tWHERE V1.code=V2.code)ORDER BY V.code ASC;" ;
        try{
            Statement st= this.connection.createStatement();
            rs =st.executeQuery(query);
            while(rs.next()){
                Integer code= rs.getInt("code");
                String vaccinename = rs.getString("vaccinename");
                String type = rs.getString("type");

                Vaccine obje = new Vaccine(code, vaccinename , type);
                reslist.add(obje);

            }
            st.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        Vaccine[] resarray = new Vaccine[reslist.size()];


        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        ResultSet rs;
        ArrayList<QueryResult.UserIDuserNameAddressResult> reslist = new ArrayList<>();

        /*String query = "SELECT U.userID , U.userName , U.address \n" +
                "FROM User U" +
                "WHERE U.userID IN (\n" +
                "\t SELECT V.userID\n" +
                "\t FROM Vaccination V\n" +
                "\t WHERE V.vacdate > ?\n" +
                "\t GROUP BY V.userID\n" +
                "\t HAVING COUNT(*) =2) ORDER BY U.userID ASC;" ;*/

        try{
            PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE U.userID IN (SELECT DISTINCT V.userID \n"  +
                    "\t FROM vaccination V\n" +
                    "\t WHERE V.vacdate > ?\n" +
                    "\t GROUP BY V.userID\n" +
                    "\t HAVING COUNT(*) =2) ORDER BY U.userID ASC;");
            stmt.setString(1,vacdate);

            rs= stmt.executeQuery();

            while(rs.next()){
                String userid = rs.getString("userID");
                String username = rs.getString("userName");
                String address = rs.getString("address") ;

                //int userid = Integer.parseInt(s);
                QueryResult.UserIDuserNameAddressResult obj = new QueryResult.UserIDuserNameAddressResult(userid , username , address);
                reslist.add(obj);
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();}

        QueryResult.UserIDuserNameAddressResult[] resarray = new QueryResult.UserIDuserNameAddressResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        ResultSet rs;
        ArrayList<Vaccine> reslist = new ArrayList<>();

        try{

            /* "\t FROM vaccination v2) AND NOT EXISTS (SELECT v3.code\n" +
                    "\tFROM vaccination v3 , vaccination v4 , vaccination v5 , vaccine v6 , vaccine v7\n" +
                    "\tWHERE v6.vaccinename NOT LIKE ? AND v6.code=v4.code AND v7.vaccinename NOT LIKE ? AND v7.code=v5.code AND v3.code=v.code AND v4.code!=v.code AND v5.code!=v.code AND !(v4.code=v5.code AND v4.userID=v5.userID AND v4.dose=v5.dose) AND v4.vacdate>v3.vacdate AND v5.vacdate>v3.vacdate);");*/
            String s = "%vac%";
            /*PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT v2.code,  v2.vaccinename, v1.vacdate\n" +
                    "FROM vaccination v1 , vaccine v2\n" +
                    "WHERE v1.code=v2.code AND v2.vaccinename NOT LIKE ? ORDER BY v1.vacdate DESC\n; " );
*/
            PreparedStatement stmt = this.connection.prepareStatement("SELECT v1.code , v1.vaccinename , v1.type\n" +
                    "FROM vaccine v1 , vaccination v2\n" +
                    "WHERE v1.vaccinename NOT LIKE ? AND v1.code=v2.code AND v2.vacdate = (SELECT MAX(v3.vacdate) \n" +
                    "\tFROM vaccination v3\n" +
                    "\tWHERE v3.code=v2.code\n" +
                    "\tGROUP BY v3.code) GROUP BY v1.code , v2.vacdate " +
                    "ORDER BY v2.vacdate DESC;");

            stmt.setString(1,s);




            rs= stmt.executeQuery();

            int x=0;
            while(rs.next() && x<2 ){
                Integer code = rs.getInt("code");
                String vname = rs.getString("vaccinename");
                String type = rs.getString("type");
                //String date = rs.getString("vacdate");

                Vaccine obj = new Vaccine(code , vname ,  type);
                reslist.add(obj);
                x++;
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        Vaccine[] resarray = new Vaccine[reslist.size()];
        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        ResultSet rs;
        ArrayList<QueryResult.UserIDuserNameAddressResult> reslist = new ArrayList<>();
        try{/*"SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE U.userID IN (SELECT V.userID \n"  +
                    "\t FROM vaccination V\n" +
                    "\t GROUP BY V.userID\n" +
                    "\t HAVING COUNT(*) >=2)\n" +
                    "INTERSECT \n" +
                    "SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE U.userID IN(\n" +
                    "\t SELECT u2.userID\n" +
                    "\t FROM user u2\n" +
                    "\t WHERE UNIQUE (SELECT s.userID\n" +
                    "\t\tFROM seen s\n" +
                    "\t\tWHERE s.userID =u2.userID))\n" +
                    "ORDER BY U.userID ");*/
            PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE U.userID IN (SELECT V.userID \n"  +
                    "\t FROM vaccination V\n" +
                    "\t GROUP BY V.userID\n" +
                    "\t HAVING COUNT(*) >=2)" +
                    " AND (U.userID IN( \n" +
                    "\t SELECT s.userID\n" +
                    "\t FROM seen s\n" +
                    "\t GROUP BY s.userID\n" +
                    "\t HAVING COUNT(*) =1)\n" +
                    " OR U.UserID NOT IN(SELECT s2.userID\n" +
                    "\t FROM seen s2))" +
                    "ORDER BY U.userID ");

            rs= stmt.executeQuery();

            while(rs.next()){
                String userid = rs.getString("userID");
                String username = rs.getString("userName");
                String address = rs.getString("address") ;

                //int userid = Integer.parseInt(s);
                QueryResult.UserIDuserNameAddressResult obj = new QueryResult.UserIDuserNameAddressResult(userid , username , address);
                reslist.add(obj);
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();}

        QueryResult.UserIDuserNameAddressResult[] resarray = new QueryResult.UserIDuserNameAddressResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {

        ResultSet rs;
        ArrayList<QueryResult.UserIDuserNameAddressResult> reslist = new ArrayList<>();

        try{
            PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE NOT EXISTS (SELECT V.code \n"  +
                    "\t FROM vaccine V , allergicsideeffect a , seen s\n" +
                    "\t WHERE a.effectname=? AND a.effectcode=s.effectcode AND s.code=V.code AND NOT EXISTS (SELECT v2.code\n" +
                    "\t\tFROM vaccination v2\n" +
                    "\t\tWHERE v2.code = V.code AND v2.userID=U.userID)) ORDER BY userID ASC; " );
            stmt.setString(1,effectname);

            rs= stmt.executeQuery();

            while(rs.next()){
                String userid = rs.getString("userID");
                String username = rs.getString("userName");
                String address = rs.getString("address") ;

                //int userid = Integer.parseInt(s);
                QueryResult.UserIDuserNameAddressResult obj = new QueryResult.UserIDuserNameAddressResult(userid , username , address);
                reslist.add(obj);
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();}

        QueryResult.UserIDuserNameAddressResult[] resarray = new QueryResult.UserIDuserNameAddressResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        ResultSet rs;
        ArrayList<QueryResult.UserIDuserNameAddressResult> reslist = new ArrayList<>();


        try{
            PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT U.userID , U.userName , U.address \n" +
                    "FROM user U\n" +
                    "WHERE U.userID IN (SELECT DISTINCT v1.userID \n"  +
                    "\t FROM vaccination v1, vaccination v2\n" +
                    "\t WHERE v1.userID=v2.userID AND v1.vacdate>? AND v1.vacdate<? AND v2.vacdate>? AND v2.vacdate<? AND EXISTS(SELECT v3.code\n" +
                    "\t\tFROM vaccine v3 , vaccine v4 \n" +
                    "\t\tWHERE v1.code=v3.code AND v2.code=v4.code AND v3.type != v4.type)) ORDER BY U.userID;" );
            stmt.setString(1,startdate);
            stmt.setString(2,enddate);
            stmt.setString(3,startdate);
            stmt.setString(4,enddate);

            rs= stmt.executeQuery();

            while(rs.next()){
                String userid = rs.getString("userID");
                String username = rs.getString("userName");
                String address = rs.getString("address") ;

                //int userid = Integer.parseInt(s);
                QueryResult.UserIDuserNameAddressResult obj = new QueryResult.UserIDuserNameAddressResult(userid , username , address);
                reslist.add(obj);
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();}

        QueryResult.UserIDuserNameAddressResult[] resarray = new QueryResult.UserIDuserNameAddressResult[reslist.size()];

        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        ResultSet rs;
        ArrayList<AllergicSideEffect> reslist = new ArrayList<>();

        try{

            PreparedStatement stmt = this.connection.prepareStatement("SELECT a.effectcode , a.effectname\n" +
                    "FROM allergicsideeffect a , seen s \n" +
                    "WHERE a.effectcode=s.effectcode AND s.userID IN (SELECT v1.userID\n" +
                            "\tFROM vaccination v1 , vaccination v2\n" +
                            "\tWHERE v1.userID=v2.userID AND DATEDIFF(v1.vacdate, v2.vacdate)>0 AND DATEDIFF(v1.vacdate, v2.vacdate)<20) ORDER BY effectcode ASC ;");
            rs= stmt.executeQuery();

            while(rs.next()){
                Integer code = rs.getInt("effectcode");
                String name = rs.getString("effectname");

                AllergicSideEffect obj = new AllergicSideEffect(code , name);
                reslist.add(obj);
            }
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        AllergicSideEffect[] resarray = new AllergicSideEffect[reslist.size()];
        return reslist.toArray(resarray);
    }

    @java.lang.Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {
        ResultSet rs;
        Double cevap=0.0;
        try{
            PreparedStatement stmt = this.connection.prepareStatement("SELECT DISTINCT AVG(v.dose) AS cvp\n" +
                    "FROM user u , vaccination v\n" +
                    "WHERE u.userID=v.userID AND u.age>65 AND NOT EXISTS(SELECT v2.userID\n " +
                    "\tFROM vaccination v2\n" +
                            "\t WHERE v.userID=v2.userID AND v2.dose>v.dose);");

            rs= stmt.executeQuery();
            rs.next();
            cevap= rs.getDouble("cvp");

                //int userid = Integer.parseInt(s);
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();}


        return cevap;
    }

    @java.lang.Override
    public int updateStatusToEligible(String givendate) {

        int numberofRowsAffected = 0;

        try {

            PreparedStatement stmt=this.connection.prepareStatement("UPDATE user u\n" +
                    "SET    u.status= ? \n" +
                    "WHERE u.status != ? AND u.userID IN (SELECT v2.userID\n" +
                    "\tFROM vaccination v2) AND\n" +
                    " u.userID NOT IN(SELECT v.userID\n" +
                    "\tFROM vaccination v\n" +
                    "\tWHERE DATEDIFF(?,v.vacdate)<120);");
            stmt.setString(1,"eligible");
            stmt.setString(2,"eligible");
            stmt.setString(3,givendate);


            numberofRowsAffected=stmt.executeUpdate();

            //close
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberofRowsAffected;
    }

    @java.lang.Override
    public Vaccine deleteVaccine(String vaccineName) {
        ResultSet rs;
        ArrayList<Vaccine> reslist = new ArrayList<>();

        try {

            PreparedStatement stmt=this.connection.prepareStatement("SELECT DISTINCT v.code , v.vaccinename , v.type\n" +
                    "FROM vaccine v\n" +
                    "WHERE v.vaccinename =?");
            stmt.setString(1,vaccineName);
            rs = stmt.executeQuery();

            rs.next();

            Integer code = rs.getInt("code");
            String name = rs.getString("vaccinename");
            String type = rs.getString("type");


            Vaccine obj = new Vaccine(code , name , type);

            try {

                PreparedStatement stmt2=this.connection.prepareStatement("delete from vaccine where vaccinename=?");
                stmt2.setString(1,vaccineName);

                stmt2.executeUpdate();

                //close
                stmt2.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }

            reslist.add(obj);
            //Close
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return reslist.get(0);
    }
}
