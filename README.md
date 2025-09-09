# utl-reading-and-writing-parquet-file-using-drop-downs-to-r-and-python
Reading and writing parquet file using drop downs to r and python
    %let pgm=utl-reading-and-writing-parquet-file-using-drop-downs-to-r-and-python;

    %stop_submission;

    Reading and writing parquet file using drop downs to r and python

    TWO SOLUTIONS

     1 r arrow
       (needed sqldf to convert read parquet to r dataframe sas could read)
     2 python pandas
     3 Note powershell and read and write parquet files

    github
    https://tinyurl.com/3e8rvmh6
    https://github.com/rogerjdeangelis/utl-reading-and-writing-parquet-file-using-drop-downs-to-r-and-python

    /*************************************************************************************************************************/
    /* INPUT                | PROCESS                                      |  OUTPUT                                         */
    /* =====                | =======                                      |  ======                                         */
    /* SD1.HAVE             | 1 R ARROW                                    |  SAS>R DATAFRAME>PARQUET                        */
    /*  NAME   SEX AGE      | =========                                    |                                                 */
    /*                      |                                              |                                                 */
    /* Alfred   M   14      | proc datasets lib=sd1                        |  ASCII Flatfile Ruler & Hex                     */
    /* Alice    F   13      |   nolist nodetails;                          |  utlrulr                                        */
    /* Barbara  F   13      |  delete want;                                |  d:/txt/have.parquet                            */
    /* Carol    F   14      | run;quit;                                    |  d:/txt/have.txt                                */
    /* Henry    M   14      |                                              |                                                 */
    /* James    M   12      | %utl_rbeginx;                                |                                                 */
    /*                      | parmcards4;                                  |  ---RecordNumber---1---RecordLength----30       */
    /* options              | library(arrow)                               |                                                 */
    /* validvarname=upcase; | library(haven)                               |    ...J.NL.......%.....Setosa                   */
    /* libname sd1 "d:/sd1";| library(sqldf)                               |  1.. ....10...15...20...25...3                  */
    /* data sd1.have;       | source("c:/oto/fn_tosas9x.R")                |  545310141441010100290000567676                 */
    /*   input              | have<-read_sas("d:/sd1/have.sas7bdat")       |  012154  5EC5650200506000354F31                 */
    /*     name$            | print(have)                                  |            umber ---  2   ---  Record Length ---*/
    /* cards4;              | write_parquet(have,"d:/txt/have.parquet")    |                                                 */
    /* Alfred  M 14         | want<-read_parquet("d:/txt/have.parquet")    |  ...Versicolor.. 0...25...30...35...4           */
    /* Barbara F 13         | str(want)                                    |  0005677666667000056766666610111221A01110       */
    /* Carol   F 14         | want<-sqldf('select * from want')            |  000652393FCF2900069279E931505C50C5C25056       */
    /* Henry   M 14         | fn_tosas9x(                                  |                                                 */
    /* James   M 12         |       inp    = want                          |  .....4........d.d.d.........L.F.........       */
    /* ;;;;                 |      ,outlib ="d:/sd1/"                      |  0...45...50...55...60...65...70...75...8       */
    /* run;quit;            |      ,outdsn ="want"                         |  1000030000A000606060101B01A0414101009000       */
    /*                      |      )                                       |  5600E43000C21240414254504562C56502008200       */
    /*                      | ;;;;                                         |  ....                                           */
    /*                      | %utl_rendx;                                  |                                                 */
    /*                      |                                              |  ---RecordNumber---3---RecordLength----281      */
    /*                      | proc print data=sd1.want;                    |                                                 */
    /*                      | run;quit;                                    |  .MXEU.%,...C.a.Vi..l..u..U..q.'I..U..^S.       */
    /*                      |                                              |  1...5....10...15...20...25...30...35...4       */
    /*                      |                                              |  14545D221A04B6156CA6187015987D241C5C055C       */
    /*                      |                                              |  4D85525C25D361769F3C075665771579745A7E35       */
    /*                      |                                              |                                                 */
    /*                      |                                              |                                                 */
    /*                      |                                              |  PARQUET>R DATAFRAME> SAS                       */
    /*                      |                                              |                                                 */
    /*                      |                                              |  SD1.HAVE                                       */
    /*                      |                                              |   NAME   SEX AGE                                */
    /*                      |                                              |                                                 */
    /*                      |                                              |  Alfred   M   14                                */
    /*                      |                                              |  Alice    F   13                                */
    /*                      |                                              |  Barbara  F   13                                */
    /*                      |                                              |  Carol    F   14                                */
    /*                      |                                              |  Henry    M   14                                */
    /*                      |                                              |  James    M   12                                */
    /*                      |                                              |                                                 */
    /*                      |                                              |                                                 */
    /*                      |------------------------------------------------------------------------------------------------*/
    /*                      |                                              |                                                 */
    /*                      | 2 PYTHON PANDAS                              |                                                 */
    /*                      | ===============                              | ASCII Flatfile Ruler & Hex                      */
    /*                      |                                              | utlrulr                                         */
    /*                      | proc datasets lib=sd1 nolist nodetails;      | d:/txt/havepy.parquet                           */
    /*                      |  delete pywant;                              | d:/txt/havepy.txt                               */
    /*                      | run;quit;                                    |                                                 */
    /*                      |                                              |                                                 */
    /*                      | %utl_pybeginx;                               | ---RecordNumber---1---RecordLength----32        */
    /*                      | parmcards4;                                  |                                                 */
    /*                      | import pandas as pd                          | PAR1...r.nL.......9(....Alfred..                */
    /*                      | exec(open('c:/oto/fn_pythonx.py').read());   | 1...5....10...15...20...25...30.                */
    /*                      | have,meta = \                                | 54531017164101010032000046676600                */
    /*                      |  ps.read_sas7bdat('d:/sd1/have.sas7bdat');   | 012154525EC5C502009860001C625455                */
    /*                      | have.to_parquet('d:/txt/havepy.parquet')     |                                                 */
    /*                      | want=pd.read_parquet('d:/txt/havepy.parquet')|                                                 */
    /*                      | print(want);                                 | ---RecordNumber---2---RecordLength----103       */
    /*                      | fn_tosas9x(                                  |                                                 */
    /*                      |    want   \                                  | 4ice....Barbara...Carol..4Henry....James        */
    /*                      |   ,outlib='d:/sd1/' \                        | 1...5....10...15...20...25...30...35...4        */
    /*                      |   ,outdsn='pywant' \                         | 3666000046766760114676600346677000046667        */
    /*                      |   ,timeest=3);                               | 493570002122121140312FC19485E295000A1D53        */
    /*                      | ;;;;                                         |                                                 */
    /*                      | %utl_pyendx;                                 |                                                 */
    /*                      |                                              | ....................Jame...Alfr.........        */
    /*                      | proc print data=sd1.pywant;                  | 0...45...50...55...60...65...70...75...8        */
    /*                      | run;quit;                                    | 1011112101110101302046667104667660000200        */
    /*                      |                                              | 50565AC5C505656C6085A1D53861C6254000B820        */
    /*                      |                                              |                                                 */
    /*                      |                                              |                                                 */
    /*                      |                                              | PARQUET>PANDAS DATAFRAME> SAS                   */
    /*                      |                                              |                                                 */
    /*                      |                                              | SD1.HAVE                                        */
    /*                      |                                              |  NAME   SEX AGE                                 */
    /*                      |                                              |                                                 */
    /*                      |                                              | Alfred   M   14                                 */
    /*                      |                                              | Alice    F   13                                 */
    /*                      |                                              | Barbara  F   13                                 */
    /*                      |                                              | Carol    F   14                                 */
    /*                      |                                              | Henry    M   14                                 */
    /*                      |                                              | James    M   12                                 */
    /*                      |                                              |                                                 */
    /*************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options
    validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      input
        name$
    cards4;
    Alfred  M 14
    Barbara F 13
    Carol   F 14
    Henry   M 14
    James   M 12
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*   NAME      SEX    AGE                                                                                                 */
    /*  Alfred      M      14                                                                                                 */
    /*  Alice       F      13                                                                                                 */
    /*  Barbara     F      13                                                                                                 */
    /*  Carol       F      14                                                                                                 */
    /*  Henry       M      14                                                                                                 */
    /*  James       M      12                                                                                                 */
    /**************************************************************************************************************************/

    /*
    / |   __ _ _ __ _ __ _____      __
    | |  / _` | `__| `__/ _ \ \ /\ / /
    | | | (_| | |  | | | (_) \ V  V /
    |_|  \__,_|_|  |_|  \___/ \_/\_/

    */

    proc datasets lib=sd1
      nolist nodetails;
     delete want;
    run;quit;

    %utl_rbeginx;
    parmcards4;
    library(arrow)
    library(haven)
    library(sqldf)
    source("c:/oto/fn_tosas9x.R")
    have<-read_sas("d:/sd1/have.sas7bdat")
    print(have)
    write_parquet(have,"d:/txt/have.parquet")
    want<-read_parquet("d:/txt/have.parquet")
    str(want)
    want<-sqldf('select * from want')
    fn_tosas9x(
          inp    = want
         ,outlib ="d:/sd1/"
         ,outdsn ="want"
         )
    ;;;;
    %utl_rendx;

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /* ASCII Flatfile Ruler & Hex                                                                                             */
    /* utlrulr                                                                                                                */
    /* d:/txt/havepy.parquet                                                                                                  */
    /* d:/txt/havepy.txt                                                                                                      */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ---RecordNumber---1---RecordLength----32                                                                               */
    /*                                                                                                                        */
    /* PAR1...r.nL.......9(....Alfred..                                                                                       */
    /* 1...5....10...15...20...25...30.                                                                                       */
    /* 54531017164101010032000046676600                                                                                       */
    /* 012154525EC5C502009860001C625455                                                                                       */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ---RecordNumber---2---RecordLength----103                                                                              */
    /*                                                                                                                        */
    /* 4ice....Barbara...Carol..4Henry....James                                                                               */
    /* 1...5....10...15...20...25...30...35...4                                                                               */
    /* 3666000046766760114676600346677000046667                                                                               */
    /* 493570002122121140312FC19485E295000A1D53                                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ....................Jame...Alfr.........                                                                               */
    /* 0...45...50...55...60...65...70...75...8                                                                               */
    /* 1011112101110101302046667104667660000200                                                                               */
    /* 50565AC5C505656C6085A1D53861C6254000B820                                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* PARQUET>PANDAS DATAFRAME> SAS                                                                                          */
    /*                                                                                                                        */
    /* SD1.HAVE                                                                                                               */
    /*  NAME   SEX AGE                                                                                                        */
    /*                                                                                                                        */
    /* Alfred   M   14                                                                                                        */
    /* Alice    F   13                                                                                                        */
    /* Barbara  F   13                                                                                                        */
    /* Carol    F   14                                                                                                        */
    /* Henry    M   14                                                                                                        */
    /* James    M   12                                                                                                        */
    /**************************************************************************************************************************/

    /*___                _   _                                         _
    |___ \   _ __  _   _| |_| |__   ___  _ __    _ __   __ _ _ __   __| | __ _ ___
      __) | | `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \ / _` | `_ \ / _` |/ _` / __|
     / __/  | |_) | |_| | |_| | | | (_) | | | | | |_) | (_| | | | | (_| | (_| \__ \
    |_____| | .__/ \__, |\__|_| |_|\___/|_| |_| | .__/ \__,_|_| |_|\__,_|\__,_|___/
            |_|    |___/                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;
     delete pywant;
    run;quit;

    %utl_pybeginx;
    parmcards4;
    import pandas as pd
    exec(open('c:/oto/fn_pythonx.py').read());
    have,meta = \
     ps.read_sas7bdat('d:/sd1/have.sas7bdat');
    have.to_parquet('d:/txt/havepy.parquet')
    want=pd.read_parquet('d:/txt/havepy.parquet')
    print(want);
    fn_tosas9x(
       want   \
      ,outlib='d:/sd1/' \
      ,outdsn='pywant' \
      ,timeest=3);
    ;;;;
    %utl_pyendx;

    proc print data=sd1.pywant;
    run;quit;

    /**************************************************************************************************************************/
    /* ASCII Flatfile Ruler & Hex                                                                                             */
    /* utlrulr                                                                                                                */
    /* d:/txt/havepy.parquet                                                                                                  */
    /* d:/txt/havepy.txt                                                                                                      */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ---RecordNumber---1---RecordLength----32                                                                               */
    /*                                                                                                                        */
    /* PAR1...r.nL.......9(....Alfred..                                                                                       */
    /* 1...5....10...15...20...25...30.                                                                                       */
    /* 54531017164101010032000046676600                                                                                       */
    /* 012154525EC5C502009860001C625455                                                                                       */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ---RecordNumber---2---RecordLength----103                                                                              */
    /*                                                                                                                        */
    /* 4ice....Barbara...Carol..4Henry....James                                                                               */
    /* 1...5....10...15...20...25...30...35...4                                                                               */
    /* 3666000046766760114676600346677000046667                                                                               */
    /* 493570002122121140312FC19485E295000A1D53                                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* ....................Jame...Alfr.........                                                                               */
    /* 0...45...50...55...60...65...70...75...8                                                                               */
    /* 1011112101110101302046667104667660000200                                                                               */
    /* 50565AC5C505656C6085A1D53861C6254000B820                                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* PARQUET>PANDAS DATAFRAME> SAS                                                                                          */
    /*                                                                                                                        */
    /* SD1.HAVE                                                                                                               */
    /*  NAME   SEX AGE                                                                                                        */
    /*                                                                                                                        */
    /* Alfred   M   14                                                                                                        */
    /* Alice    F   13                                                                                                        */
    /* Barbara  F   13                                                                                                        */
    /* Carol    F   14                                                                                                        */
    /* Henry    M   14                                                                                                        */
    /* James    M   12                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
