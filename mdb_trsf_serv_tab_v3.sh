#!/bin/bash

v_usr="spider_user"
v_pwd="THG0896-ght"

v_linux_user=$USER
if [[ $v_linux_user == "" ]]
then
  v_linux_user="root"
fi

v_path_log="/var/lib/mdb_TOOLS/synchro/log"
v_path_sql="/var/lib/mdb_TOOLS/synchro/sql_script"

v_path_csv="/var/lib/mdb_TOOLS/synchro/csv_file"
cd $v_path_csv

# ********************************************************************************************************************************************
# ********************************************************************************************************************************************
# ** DEBUT DES FONCTIONS ** DEBUT DES FONCTIONS ** DEBUT DES FONCTIONS ** DEBUT DES FONCTIONS ** DEBUT DES FONCTIONS ** DEBUT DES FONCTIONS **
# ********************************************************************************************************************************************
# ********************************************************************************************************************************************

# ***************************************************************************************************************** FONCTION PROCESS EXISTS **
function f_process_exists ()
{
  echo "-- recherche de process à risque (conflit avec synchro) : "$1
  v_nb_process=$(ps -aux | grep -i $1 | grep -v grep | wc -l)
  if [[ $v_nb_process -gt "0" ]]
  then
    v_statut="CONFLIT"
    f_aff_erreur "présence de "$v_nb_process" process "$1" : arrêt synchronisation car risque de CONFLIT"
  fi
}

# ******************************************************************************************************* FONCTION AWK SEARCH STRING IN FILE **
function f_awk_search_string_in_file ()
{
  awk -v v_search="$1" '$0~v_search {print $1}' $2
}
# ****************************************************************************************************************** FONCTION JOURNALISATION **
function f_journalisation ()
{
  if [[ $v_nb_rows_source_tab == "" ]]
  then
    v_nb_rows_source_tab=0
  fi
  if [[ $v_nb_rows_target_tab == "" ]]
  then
    v_nb_rows_target_tab=0
  fi
  if [[ $v_nb_col_ai_mcs == "" ]]
  then
    v_nb_col_ai_mcs=0
  fi
  if [[ $v_nb_col_ai_idb == "" ]]
  then
    v_nb_col_ai_idb=0
  fi
  if [[ $v_nb_col_geometry == "" ]]
  then
    v_nb_col_geometry=0
  fi
  echo "-- journalisation opérations de synchro"
  if [[ $v_process == "REQ" ]]
  then
    local j_sql="UPDATE _serv_transfert._transf_tab_serv_to_serv_request_idb set nb_rows_source = "$v_nb_rows_source_tab", nb_rows_cible = "$v_nb_rows_target_tab", trsf_log = \""${v_cpimport_log}"\", start_dt = '"$start_time"', end_dt = '"$end_time"', duration = timediff('"$end_time"', '"$start_time"'), comp_struct_source_vs_cible = '"$v_struct_tab_source_vs_target"', error_msg = \""${v_error_msg}"\" "$v_maj_booleen_tab_req" WHERE id_ai = "$v_id_ai" ; "
    local j_opt="--verbose"
    local j_db="target"
    f_mdb_sql_query_exec $j_opt "${j_sql}" $j_db
  elif [[ $v_process == "STD" ]]
  then
    local j_sql="insert into _serv_transfert._transf_tab_serv_to_serv_idb(serv_source,db_source,tab_source,serv_cible,db_cible,tab_cible,nb_rows_source,nb_rows_cible,start_dt,end_dt,comp_struct_source_vs_cible,error_msg) values ('"$v_source_serv_name_upper"','"$v_source_db"','"$v_source_tab"','"$v_target_serv_name_upper"','"$v_target_db"','"$v_target_tab"',"$v_nb_rows_source_tab","$v_nb_rows_target_tab",'"$start_time"','"$end_time"','"$v_struct_tab_source_vs_target"',\""${v_error_msg}"\") ; "
    local j_opt="--verbose"
    local j_db="target"
    f_mdb_sql_query_exec $j_opt "${j_sql}" $j_db
  elif [[ $v_process == "SYN" ]]
  then
# ajout colonnes : MIN/MAX id_ai (source et cible), geometry type (verif dans bano, distance mairie, inclus dans polygone du code insee)
    v_struct_target_tab=$(sed 's/"//g' <<< $v_struct_target_tab)
    local j_sql="insert into _serv_transfert._transf_db1_to_db2_replication_idb(freq,serv_source,db_source,tab_source,serv_cible,db_cible,tab_cible,nb_rows_source,nb_rows_cible,trsf_log,start_dt,end_dt,struct_source_vs_cible_avant_repli,error_msg,ddl_create_table,b_geometry,b_id_ai,b_done) values ('"$v_freq"','"$v_source_serv_name_upper"','"$v_source_db"','"$v_source_tab"','"$v_target_serv_name_upper"','"$v_target_db"','"$v_target_tab"',"$v_nb_rows_source_tab","$v_nb_rows_target_tab",\""${v_cpimport_log}"\",'"$start_time"','"$end_time"','"$v_struct_tab_source_vs_target"',\""${v_error_msg}"\",\""${v_struct_target_tab=}"\","$v_nb_col_geometry",("$v_nb_col_ai_mcs+$v_nb_col_ai_idb"),1) ; "
    local j_opt="--verbose"
    local j_db="target"
    f_mdb_sql_query_exec $j_opt "${j_sql}" $j_db
  fi
  echo ""
}

# ****************************************************************************************************************** FONCTION AFFICHAGE ERREUR **
function f_aff_erreur ()
{
  v_error_msg="ERREUR : "$1
  # echo ""
  echo $v_error_msg
  printf '\7'
  v_maj_booleen_tab_req=", b_done = 1, b_restart = 0, b_force = 0"
  f_journalisation
  if [[ $v_process == "REQ" ]]
  then
    v_sql="SELECT demand_dt, IFNULL(duration, '') FROM _serv_transfert._transf_tab_serv_to_serv_request_idb WHERE id_ai = "$v_id_ai" ; "
    v_opt="--quick --skip-column-names"
    v_db="target"
    v_res=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    v_demande=$(awk -F'\t' '{ print $1 }' <<< $v_res)
    v_duree=$(awk -F'\t' '{ print $2 }' <<< $v_res)
    v_body="Bonjour,

SOURCE   - serveur : ${v_source_serv_name_upper}  - bdd : ${v_source_db}  - table : ${v_source_tab}
CIBLE    - serveur : ${v_target_serv_name_upper}  - bdd : ${v_target_db}  - table : ${v_target_tab}

horodatage demande : ${v_demande}

début exécution    : ${start_time}
fin exécution      : ${end_time}
durée transfert    : ${v_duree}

statut transfert   : ${v_statut}
tLignes transférées : ${v_nb_rows_target_tab} (vs. ${v_nb_rows_source_tab} lignes dans la table source)

message d'erreur   : ${v_error_msg}
message warning    : ${v_warning_msg}

Bien cordialement,"

    v_subject="TRANSFERT TABLE DE SERVEUR A SERVEUR"
    v_email_to=$v_email_demandeur
    v_email_cc="gerardin.thierry@gmail.com"
    f_envoi_email
  fi
  exit
}

# ****************************************************************************************************************** FONCTION AFFICHAGE WARNING **
function f_aff_warning ()
{
  v_warning_msg="-- WARNING : "$1
  # echo ""
  echo $v_warning_msg
}
# ****************************************************************************************************************** FONCTION AUTO_INCREMENT COLMN **
function f_auto_increment_column ()
{
  echo "-- recherche présence colonne auto-increment"
  v_sql="SELECT COUNT(*) FROM information_schema.COLUMNS AS isc WHERE isc.table_schema='"$v_source_db"' AND isc.table_name='"$v_source_tab"' AND isc.extra LIKE 'auto_increment' ; "
  v_opt="--quick --skip-column-names"
  v_db="source"
  v_nb_col_ai_idb=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
  if [[ $v_nb_col_ai_idb == "" ]]
  then
    v_nb_col_ai_idb="0"
  fi
  if [[ $v_nb_col_ai_idb -eq "0" ]]
  then
    v_sql="SELECT COUNT(*) FROM information_schema.COLUMNS AS isc WHERE isc.table_schema='"$v_source_db"' AND isc.table_name='"$v_source_tab"' AND isc.column_comment LIKE 'autoincrement%' ; "
    v_opt="--quick --skip-column-names"
    v_db="source"
    v_nb_col_ai_mcs=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    if [[ $v_nb_col_ai_mcs == "" ]]
    then
      v_nb_col_ai_mcs="0"
    fi
  fi
  if [[ $v_nb_col_ai_idb -gt "0" ]]
  then
    v_sql="SELECT isc.column_name FROM information_schema.COLUMNS AS isc WHERE isc.table_schema='"$v_source_db"' AND isc.table_name='"$v_source_tab"' AND isc.extra LIKE 'auto_increment' ; "
    v_opt="--quick --skip-column-names"
    v_db="source"
    v_source_col_ai=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    echo "-- colonne AUTO_INCREMENT source : "$v_source_col_ai

    v_sql="SELECT (MAX("$v_source_col_ai") + 1) FROM "$v_source_tab" ; "
    v_opt="--quick --skip-column-names"
    v_db="source"
    v_source_max_ai=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    echo "-- MAX(id_ai)+1 table source : "$v_source_max_ai
  elif [[ $v_nb_col_ai_mcs -gt "0" ]]
  then
    v_sql="SELECT column_name FROM information_schema.COLUMNS AS isc WHERE isc.table_schema='"$v_source_db"' AND isc.table_name='"$v_source_tab"' AND isc.column_comment LIKE 'autoincrement%' ; "
    v_opt="--quick --skip-column-names"
    v_db="source"
    v_source_col_ai=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    echo "-- colonne AUTO_INCREMENT source : "$v_source_col_ai

    v_sql="SELECT (MAX("$v_source_col_ai") + 1) FROM "$v_source_tab" ; "
    v_opt="--quick --skip-column-names"
    v_db="source"
    v_source_max_ai=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    echo "-- MAX(id_ai)+1 table source : "$v_source_max_ai
  fi
}
# ****************************************************************************************************************** FONCTION GEOMETRY COLUMN **
function f_geometry_column ()
{
  echo "-- recherche présence colonnes géométriques"
  v_sql="SELECT COUNT(*) FROM information_schema.GEOMETRY_COLUMNS WHERE g_table_schema='"$v_source_db"' AND g_table_name='"$v_source_tab"' ; "
  v_opt="--quick --skip-column-names"
  v_db="source"
  v_nb_col_geometry=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
  if [[ $v_nb_col_geometry == "" ]]
  then
    v_nb_col_geometry="0"
  fi
  if [[ $v_nb_col_geometry -gt "0" ]]
  then
    v_sql="SELECT g_geometry_column FROM information_schema.GEOMETRY_COLUMNS WHERE g_table_schema='"$v_source_db"' AND g_table_name='"$v_source_tab"' ; "
    v_opt="--quick --skip-column-names"
    v_db="source"

    v_source_col_geometry=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
    v_target_col_text_geo=$v_source_col_geometry"_text"
    echo "-- colonne GEOMETRY source : "$v_source_col_geometry
    echo "-- colonne GEOMETRY cible  : "$v_target_col_text_geo

  fi
}

function f_structure_tab ()
{
  if [[ $1 == "source" ]]
  then
    s_db=$v_source_db
    s_tab=$v_source_tab
    s_host=$v_source_serv_host_name
  else
    s_db=$v_target_db
    s_tab=$v_target_tab
    s_host=$v_target_serv_host_name
  fi
  # exécution de la commande SHOW CREATE TABLE
  s_opt="-q -N"
  s_sql="SHOW CREATE TABLE "$s_db"."$s_tab" ; "
  mariadb -A -u$v_usr -p$v_pwd -h$s_host $s_opt -e"${s_sql}" > $v_path_sql"/show_create_table.sql"
  # extraction de la commande CREATE TABLE
  echo "-- table "$s_tab" : extraction create table du fichier create_table.sql"
  # (cat show_create_table.sql | awk -F'\t' '{print $2}') > orig_create_table.sql
  awk -F'\t' '{print $2}' < $v_path_sql"/show_create_table.sql" > $v_path_sql"/orig_create_table.sql"
  # suppression LF du fichier create table.sql
  echo "-- suppression des sauts de lignes du fichier create_table_"$s_tab".sql"
  # (cat orig_create_table.sql | awk 1 ORS=' ') > create_table.sql
  tr -d '\n' < $v_path_sql"/orig_create_table.sql" > $v_path_sql"/create_table_"$s_tab".sql"
  sed -i 's/\\n//g' $v_path_sql"/create_table_"$s_tab".sql"
  echo "-- recherche présence caractères accentués"
  v_nb_char_accent=$(awk '/[é,è,ê,ë,à,â,ä,ô,ö,ù,û,ü,î,ï]/ {n++} END {print n}' $v_path_sql"/create_table_"$s_tab".sql")
  if [[ $v_nb_char_accent == "" ]]
  then
    v_nb_char_accent=0
  fi
  if [[ $v_nb_char_accent -eq "0" ]]
  then
    # remplacement caractères accentués du fichier create table.sql
    echo "-- substitution des caractères accentués du fichier create_table_"$s_tab".sql"
    sed -i '{y/éèêë/eeee/;y/àâä/aaa/;y/ôö/oo/;y/ùûü/uuu/;y/îï/ii/}' $v_path_sql"/create_table_"$s_tab".sql"
  fi
  # modification de l'option AUTO_INCREMENT du CREATE TABLE
  sed -i 's/AUTO_INCREMENT=[0-9]*/AUTO_INCREMENT=1/g' $v_path_sql"/create_table_"$s_tab".sql"
  # modification de la commande CREATE TABLE : retrait des (AltGr+7) entourant le nom de la table
  v_search="CREATE TABLE \`"$s_tab"\`"
  v_sub="CREATE TABLE "$s_tab""
  echo "-- substitution de '"$v_search"' par '"$v_sub"' dans le fichier create_table_"$s_tab".sql"
  sed -i "s/$v_search/$v_sub/g" $v_path_sql"/create_table_"$s_tab".sql"
  # modification du CREATE TABLE bdd.tab
  v_search="CREATE TABLE "$s_tab
  v_sub="CREATE TABLE "$s_db"."$s_tab"_new"
  sed -i "s/$v_search/$v_sub/g" $v_path_sql"/create_table_"$s_tab".sql"
  # echo $v_create_tab
  if [[ $1 == "source" ]]
  then
    cp $v_path_sql"/create_table_"$s_tab".sql" $v_path_sql"/create_table_source.sql"
    v_struct_source_tab=$(cat $v_path_sql"/create_table_source.sql")
  else
    cp $v_path_sql"/create_table_"$s_tab".sql" $v_path_sql"/create_table_target.sql"
    v_struct_target_tab=$(cat $v_path_sql"/create_table_target.sql")
  fi
  echo ""
}

# fonction d'envoi d'email utilisant l'application MUTT
function f_envoi_email ()
{
  echo "${v_body}" | mutt -s "${v_subject}" $v_email_to -c $v_email_cc
}

function f_serv_init ()
{
  if [[ $1 == "source" ]]
  then
    echo "-- initialisation serveur source"
    v_source_serv_host_name=$(echo $2 | sed 's/.*/\L&/g')
    v_source_serv_name_upper=$(echo $2 | sed 's/.*/\U&/g')
    v_source_serv_host_name="agencemd-"$v_source_serv_host_name
  else
    echo "-- initialisation serveur cible"
    v_target_serv_host_name=$(echo $2 | sed 's/.*/\L&/g')
    v_target_serv_name_upper=$(echo $2 | sed 's/.*/\U&/g')
    v_target_serv_host_name="agencemd-"$v_target_serv_host_name
  fi
}

function f_serv_check ()
{
  v_local_serv_host_name=$(echo $HOSTNAME | sed 's/.terrancle.net//g')
  if [[ $1 == "source" ]]
  then
    echo "-- controle existence serveur source dans /etc/hosts"
    if [[ $v_source_serv_host_name == $v_local_serv_host_name ]]
    then
      v_source_serv_host_name="localhost"
      v_source_serv_ip_addr="127.0.0.1"
    else
      local v_file="/etc/hosts"
      v_source_serv_ip_addr=$(f_awk_search_string_in_file $v_source_serv_host_name $v_file)
      if [[ $v_source_serv_ip_addr == "" ]]
      then
        f_aff_erreur "nom du serveur source absent du fichier /etc/hosts"
      fi
    fi
  else
    echo "-- controle existence serveur cible dans /etc/hosts"
    if [[ $v_target_serv_host_name == $v_local_serv_host_name ]]
    then
      v_target_serv_host_name="localhost"
      v_target_serv_ip_addr="127.0.0.1"
    else
      local v_file="/etc/hosts"
      v_target_serv_ip_addr=$(f_awk_search_string_in_file $v_target_serv_host_name $v_file)
      if [[ $v_target_serv_ip_addr == "" ]]
      then
        f_aff_erreur "nom du serveur cible absent du fichier /etc/hosts"
      fi
    fi
  fi
}

function f_mdb_object_exists ()
{
  echo "-- controle existence "$1" "$2
  if [[ $2 == "source" ]]
  then
    local e_db=$v_source_db
    local e_tab=$v_source_tab
    local e_host=$v_source_serv_host_name
  else
    local e_db=$v_target_db
    local e_tab=$v_target_tab
    local e_host=$v_target_serv_host_name
  fi
  if [[ $1 == "tab" ]] && [[ $2 == "source" ]]
  then
    echo ""
    echo "** -- CALCUL STRUCTURE TABLE SOURCE"
    echo "-- "$v_source_tab" - bdd : "$v_source_db" - serveur : "$v_source_serv_host_name
    # v_struct_source_tab=$(f_structure_tab source)
    f_structure_tab source
  fi
  if [[ $1 == "db" ]]
  then
    local e_opt="-q -N"
    local e_sql="SELECT EXISTS (SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '"$e_db"') ; "
    local e_db_exists=$(mariadb -A -u$v_usr -p$v_pwd -h$e_host $e_opt -e"${e_sql}" )
    if [[ $e_db_exists -eq "0" ]]
    then
      if [[ $v_process == "SYN" ]] && [[ $2 == "target" ]]
      then
        local e_sql="CREATE DATABASE "$v_target_db" COLLATE utf8mb3_general_ci"
        local e_opt="--verbose"
        local e_db="target"
        f_mdb_sql_query_exec $e_opt "${e_sql}" $e_db
        # echo ""
      else
        f_aff_erreur "la base de donnees "$e_db" n'existe pas sur le serveur : "$e_host
      fi
    fi
  elif [[ $1 == "tab" ]]
  then
    local e_opt="-q -N"
    local e_sql="SELECT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_NAME = '"$e_tab"') ; "
    local e_tab_exists=$(mariadb -A -u$v_usr -p$v_pwd -h$e_host $e_opt -e"${e_sql}" )
    if [[ $e_tab_exists -eq "0" ]]
    then
      if [[ $v_process == "SYN" ]] && [[ $2 == "target" ]]
      then
        local e_opt="--verbose"
        mariadb -A -u$v_usr -p$v_pwd -h$e_host -D$e_db $e_opt < $v_path_sql"/create_table_source.sql"  > $v_path_log"/create_table_"$e_tab".log"
      else
        f_aff_erreur "la table "$e_tab" n'existe dans aucune base de donnees sur le serveur : "$e_host
      fi
    else
      local e_opt="-q -N"
      local e_sql="SELECT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '"$e_db"' AND TABLE_NAME = '"$e_tab"') ; "
      local e_dbtab_exists=$(mariadb -A -u$v_usr -p$v_pwd -h$e_host $e_opt -e"${e_sql}" )

      if [[ $e_dbtab_exists -eq "0" ]]
      then
        if [[ $v_process == "SYN" ]] && [[ $2 == "target" ]]
        then
          local e_opt="--verbose"
          mariadb -A -u$v_usr -p$v_pwd -h$e_host -D$e_db $e_opt < $v_path_sql"/create_table_source.sql"  > $v_path_log"/create_table_"$e_tab".log"
        else
          f_aff_erreur "la table "$e_tab" existe mais pas dans la base de donnees "$e_db" sur le serveur : "$e_host
        fi
      fi
    fi
  fi
}

function f_mdb_sql_query_exec ()
{
  # premier parametre : options mariadb
  if [[ $1 == *"--quick"* ]] || [[ $1 == *"--skip_column-names"* ]] || [[ $1 == *"--verbose"* ]]
  then
    local r_opt=$1
  else
    f_aff_erreur "les options mariadb (1er parametre) doit contenir '--quick' et/ou '--quick --skip-column-names' et/ou '--verbose'"
  fi
  # deuximeme parametre : requete sql
  if [[ $2 != "" ]]
  then
    local r_sql=$2
  else
    f_aff_erreur "la requete (2eme parametre) ne peut etre vide"
  fi
  # troisieme parametre : base de donnees
  if [[ $3 == "source" ]]
  then
    local r_db=$v_source_db
    local r_host=$v_source_serv_host_name
  else
    local r_db=$v_target_db
    local r_host=$v_target_serv_host_name
  fi
  mariadb -A -u$v_usr -p$v_pwd -h$r_host ${r_opt} -D$r_db -e"${r_sql}"
}
# **************************************************************************************************************************************
# **************************************************************************************************************************************
# ** DEBUT DU CODE DU SHELL SCRIPT ** DEBUT DU CODE DU SHELL SCRIPT ** DEBUT DU CODE DU SHELL SCRIPT ** DEBUT DU CODE DU SHELL SCRIPT ** 
# **************************************************************************************************************************************
# **************************************************************************************************************************************

# test premier parametre
v_truncate=$(awk -F'-' '{ print $1 }' <<< $1)
v_process=$(awk -F'-' '{ print $2 }' <<< $1)
v_freq=$(awk -F'-' '{ print $3 }' <<< $1)
if [[ $v_truncate != "TRUNC" ]] && [[ $v_truncate != "APPEND" ]]
then
  f_aff_erreur "le 1er parametre doit etre egal a 'TRUNC' ou 'APPEND'"
fi
if [[ $v_process != "SYN" ]] && [[ $v_process != "REQ" ]] && [[ $v_process != "STD" ]]
then
  f_aff_erreur "le 1er parametre doit etre egal a 'SYN' ou 'REQ' ou 'STD'"
fi 
if [[ $v_process == "SYN" ]]
then
  v_lib_process="SYNCHRONISATION"
elif [[ $v_process == "REQ" ]]
then
  v_lib_process="TRANSFERT REQUEST"
elif [[ $v_process == "STD" ]]
then
  v_lib_process="TRANSFERT STANDARD"
fi
echo ""
echo "** -- DEBUT TRAITEMENT "$v_lib_process
echo ""

# arret synchro si sauvegarde via mydumper est en cours
f_process_exists "mydumper"

# arret synchro si sauvegarde via mydumper est en cours
f_process_exists "myloader"

# INPUT PARAMETERS

# optionnal : _serv_transfert table id_ai (ONLY in case of transfert request where the transfert_request table is used both to trigger the transfert and to log the result
v_id_ai=$8

# nom du serveur source
f_serv_init source $2
# test serveur source
f_serv_check source

# nom de la bdd source
v_source_db=$3
f_mdb_object_exists db source
# nom de la table source
v_source_tab=$4
f_mdb_object_exists tab source

# nom du serveur cible : si le nom du serveur cible n'est pas mentionne au parametre 5 alors serveur cible = serveur local
if [[ $5 == "" ]]
then
  f_serv_init target "db2"
# A DECOMMENTER
# $(echo $HOSTNAME | sed 's/agencemd-//g; s/.terrancle.net//g')
else
  f_serv_init target $5
fi

# test serveur cible
f_serv_check target

# nom de la bdd cible : si la bdd cible n'est pas mentionee au param 6 alors bdd cible = bdd source
v_target_db=$6
if [[ $v_target_db == "" ]]
then
  v_target_db=$v_source_db
  f_mdb_object_exists db target
else
  f_mdb_object_exists db target
fi

# nom de la table cible : si la table cible n'est pas mentionee au param 7 alors table cible = table source
v_target_tab=$7
if [[ $v_target_tab == "" ]]
then
  v_target_tab=$v_source_tab
  f_mdb_object_exists tab target
else
  f_mdb_object_exists tab target
fi

# email demandeur transfert request
v_email_demandeur=$9

echo ""
echo "** -- CALCUL STRUCTURE TABLE CIBLE"
echo "-- "$v_target_tab" - bdd : "$v_target_db" - serveur : "$v_target_serv_host_name
# v_struct_target_tab=$(f_structure_tab target)
f_structure_tab target
if [[ $v_struct_source_tab != $v_struct_target_tab ]]
then
  v_struct_tab_source_vs_target="DIFFERENTE"
  echo "-- comparaison des structures table source vs. table cible : DIFFERENTE"

  if [[ $v_process == "REQ" ]]
  then
    if [[ $v_freq="FORCE" ]]
    then
      f_aff_warning "transfert request + b_force = 1 : la structure de la table cible va être automatiquement alignée sur celle de la table source"
      v_target_tab_align="TRUE"
    else
      v_target_tab_align="FALSE"
      f_aff_erreur "la structure des tables source et cible est "$v_struct_tab_source_vs_target" : modifiez la colonne b_force = 1 pour aligner la structure de la table cible sur celle de la table source"
    fi
  elif [[ $v_process == "SYN" ]]
  then
    f_aff_warning "synchro MMA (de db1 à db2) : la structure de la table cible va être automatiquement alignée sur celle de la table source"
    v_target_tab_align="TRUE"
  else
    v_target_tab_align="FALSE"
    f_aff_erreur "la structure des tables source et cible est "$v_struct_tab_source_vs_target". Merci de corriger ce problème avant d'exécuter un nouveau transfert"
  fi
else
  v_target_tab_align="FALSE"
  v_struct_tab_source_vs_target="IDENTIQUE"
  echo "-- comparaison des structures table source vs. table cible : IDENTIQUE"
fi

echo ""
echo "** -- RAPPEL DES PARAMETRES"
echo "procédure        : "$1
echo "serveur source   : "$v_source_serv_host_name" - adresse IP : "$v_source_serv_ip_addr
echo "serveur cible    : "$v_target_serv_host_name" - adresse IP : "$v_target_serv_ip_addr
echo "bdd source       : "$v_source_db
echo "bdd cible        : "$v_target_db
echo "table source     : "$v_source_tab
echo "table cible      : "$v_target_tab
echo "cible vs. source : "$v_struct_tab_source_vs_target

# table upload start date-time
start_time=$(date +%F' '%T'.'%N)

echo ""
echo "-- horodatage debut transfert : "$start_time
echo ""

v_sql="SELECT LOWER(engine) FROM information_schema.TABLES WHERE table_schema = '"$v_target_db"' AND table_name = '"$v_target_tab"' ; "
v_opt="--quick --skip-column-names"
v_db="target"
v_engine=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
echo "-- determination du moteur bdd de la table cible : "$v_target_tab" - serveur : "$v_target_serv_host_name" = "$(echo $v_engine | sed 's/.*/\U&/g')

if [[ $v_truncate == "TRUNC" ]]
then
  if [[ $v_engine == "innnodb" ]]
  then
    v_search="CREATE TABLE"
    v_sub="CREATE OR REPLACE TABLE"
    sed -i "s/$v_search/$v_sub/g" $v_path_sql"/create_table_source.sql"
    if [[ $v_target_tab_align == "TRUE" ]]
    then
      mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -D$v_target_db < $v_path_sql"/create_table_source.sql" > $v_path_log"/create_table_"$v_target_tab".log"
    else
      v_sql="CREATE OR REPLACE TABLE "$v_target_db"."$v_target_tab"_new LIKE "$v_target_db"."$v_target_tab" ; CREATE OR REPLLACE TABLE IF NOT EXISTS "$v_target_db"."$v_target_tab"_new LIKE "$v_target_db"."$v_target_tab"_old ; "
      v_opt="--verbose"
      v_db="target"
      f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
    fi
  else
    echo "-- DROP table cible : "$v_target_db"."$v_target_tab"_new - serveur : "$v_target_serv_host_name
    v_sql="DROP TABLE IF EXISTS "$v_target_db"."$v_target_tab"_new ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
    echo "-- CREATE table cible : "$v_target_db"."$v_target_tab"_new - serveur : "$v_target_serv_host_name
    if [[ $v_target_tab_align == "TRUE" ]]
    then
      mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -D$v_target_db < $v_path_sql"/create_table_source.sql" > $v_path_log"/create_table_"$v_target_tab".log"
    else
      v_sql="CREATE TABLE "$v_target_db"."$v_target_tab"_new LIKE "$v_target_db"."$v_target_tab" ; CREATE TABLE IF NOT EXISTS "$v_target_db"."$v_target_tab"_new LIKE "$v_target_db"."$v_target_tab"_old ; "
      v_opt="--verbose"
      v_db="target"
      f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
    fi
  fi
#
# FUTURE USE : managge APPEND data to target table instead of truncating it
# WARNING : prevent doublons using either a UNIQUE constraint (innodb table) or an EXISTS clause (columnstore table)
# else
# echo "-- truncate table cible : NON - ATTENTION RISQUE DE DOUBLONS"
#
fi

if [[ $v_linux_user == "root" ]] && [[ $v_engine == "columnstore" ]]
then
  echo "** -- méthode SELECT | CPIMPORT"

  # process detection ; cpimport if true then exit synchro to prevent a "cpimport fork error"
  f_process_exists "cpimport"

  # get the source table column list without geometry columns as it's not supported by columnstore engine
  v_opt="--quick --skip-column-names"
  v_db="source"
  v_sql="SELECT _serv_ref.col_list_full('"$v_source_db"','"$v_source_tab"', 0, 0, 0, 0)"
  v_source_col_list=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
  echo "-- liste colonnes table source : "$v_source_col_list

  # test presence colonnes auto-incrémentées
  f_auto_increment_column

  # move the id_ai column from the begining to the end of column list
  if [[ $v_nb_col_ai_mcs -gt "0" ]]
  then
    v_search="\`"$v_source_col_ai"\`"
    v_source_first_col_in_col_list=$(awk -F', ' '{print $1}' <<< $v_source_col_list)
    if [[ $v_source_first_col_in_liste_colonnes == $v_search ]]
    then
      v_search="\`"$v_source_col_ai"\`, "
      v_sub=""
      v_source_col_list=$(sed "s/$v_search/$v_sub/g" <<< $v_source_col_list)
      v_source_col_list=$v_source_col_list", "$v_source_first_col_in_col_list
      echo "-- AUTO_INCREMENT - liste colonnes : "$v_source_col_list
    fi
  fi

# AUTO_INCREMENT=$v_min_id_ai DE SOURCE_TAB
# VERIFIER MIN ET MAX id_ai 

  # remove auto_increment property from the id_ai column
  if [[ $v_nb_col_ai_mcs -gt "0" ]]
  then
    v_sql="ALTER TABLE "$v_source_tab"_new DROP COLUMN "$v_source_col_ai" ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db

    v_sql="ALTER TABLE "$v_target_tab"_new ADD COLUMN "$v_source_col_ai" INT UNSIGNED NOT NULL DEFAULT '0' ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
  fi

  echo "-- SELECT | CPIMPORT - liste colonnes : "$v_source_col_list
  echo "-- SELECT   - from "$v_source_serv_host_name" - bdd : "$v_source_db" - table : "$v_source_tab"_new"
  echo "-- CPIMPORT - to   "$v_target_serv_host_name" - bdd : "$v_target_db" - table : "$v_target_tab"_new"
  v_sql="select "$v_source_col_list" from "$v_source_tab" ; "
  mariadb -A -u$v_usr -p$v_pwd -h$v_source_serv_host_name -q -e"${v_sql}" -N $v_source_db | cpimport -r3 -w24 -e15000 -n1 -s '\t' $v_target_db $v_target_tab"_new"  > $v_path_log"/cpimport.log"

  # recreate the id_ai column with its initial auto_increment property
  if [[ $v_nb_col_ai_mcs -gt "0" ]]
  then
    v_sql="ALTER TABLE "$v_source_tab"_new DROP COLUMN "$v_source_col_ai" ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db

    v_sql="ALTER TABLE "$v_source_tab"_new ADD COLUMN "$v_source_col_ai" INT UNSIGNED NOT NULL DEFAULT '0' COMMENT 'autoincrement=1' ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db

    # set the auto_increment value on target table = same value as source table
    v_sql="ALTER TABLE "$v_source_tab"_new AUTO_INCREMENT="$v_source_max_ai" ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
  fi

  # table upload end date-time
  end_time=$(date +%F' '%T'.'%N)

  # echo ""
  echo "-- log cpimport --"
  awk '/INFO/ && /rows inserted/ {print $0}' $v_path_log"/"cpimport.log
  awk '/completed/ && /seconds/ {print $0}' $v_path_log"/"cpimport.log
  awk '/status/ {print $0}' $v_path_log"/"cpimport.log
  echo ""
  echo "-- warnings --"
  awk '/WARN/ {print $0}' $v_path_log"/"cpimport.log

else
  v_time=$(sed 's/://g' <<< $(date +%T))
  v_date=$(sed 's/-//g' <<< $(date +%F))
  v_output_file="outfile_"$v_source_serv_host_name"_"$v_source_db"_"$v_source_tab"_"$v_date"_"$v_time".tsv"

  if [[ $v_linux_user != "root" ]]
  then
    echo "-- non-root user : "$v_linux_user
  fi
  if [[ $v_engine == "innodb" ]]
  then
    echo "-- non-columstore engine : "$v_engine
  fi
  echo "-- SELECT OUTFILE - from "$v_source_serv_host_name" - bdd : "$v_source_db" - table : "$v_source_tab

  # get the source table column list with geometry colmns as it's not supported by columnstore engine
  v_opt="--quick --skip-column-names"
  v_db="source"
  v_sql="SELECT _serv_ref.col_list_full('"$v_source_db"','"$v_source_tab"', 0, 0, 0, 1)"
  v_source_col_list=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
  echo "-- liste colonnes : "$v_liste_colonnes

  # test presence colonnes auto-incrémentées
  f_auto_increment_column

  # test presence colonnes de type geometry
  f_geometry_column

  # convert geometric column content to text
  if [[ $v_nb_col_geometry -gt "0" ]]
  then
    v_search="\`"$v_source_col_geometry"\`"
    v_sub="ST_AsText("$v_source_col_geometry")"
    echo "-- substitution de '"$v_search"' par '"$v_sub"' dans v_source_col_list"
    v_liste_col_geo_out=$(sed "s/$v_search/$v_sub/g" <<< $v_source_col_list)

    echo "-- GEOMETRY - OUTFILE liste colonnes : "$v_liste_col_geo_out

    echo "-- SELECT OUTFILE - GEOMETRY"
    v_sql="select "$v_liste_col_geo_out" from "$v_source_tab" ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_source_serv_host_name -q -e"${v_sql}" -N $v_source_db > $v_path_csv"/"$v_output_file

  elif [[ $v_nb_col_ai_idb -gt "0" ]]
  then
    echo "-- AUTO_INCREMENT - OUTFILE liste colonnes : "$v_source_col_list

    # SELECT OUTFILE AUTO_INCREMENT (géré distinctement meme si idebtique a standard pour harmoniser le code)
    echo "-- SELECT OUTFILE - AUTO_INCREMENT"
    v_sql="select "$v_source_col_list" from "$v_source_tab" ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_source_serv_host_name -q -e"${v_sql}" -N $v_source_db > $v_path_csv"/"$v_output_file

  else
    echo "-- standard - OUTFILE liste colonnes : "$v_source_col_list

    # SELECT OUTFILE STANDARD
    echo "-- SELECT OUTFILE - standard"
    v_sql="select "$v_source_col_list" from "$v_source_tab" ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_source_serv_host_name -q -e"${v_sql}" -N $v_source_db > $v_path_csv"/"$v_output_file

  fi

  # change tsv files owner and permissions to make sure mariadb will be able to read them when executing the LOAD DATA statement
  chown mysql:mysql *tsv
  chmod 775 *tsv

  echo "-- LOAD DATA LOCAL INFILE - to   "$v_target_serv_host_name" - bdd : "$v_target_db" - table : "$v_target_tab"_new"

  # remove auto_increment property from the id_ai column
  if [[ $v_nb_col_ai_idb -gt "0" ]]
  then
    v_sql="ALTER TABLE  "$v_target_db"."$v_target_tab"_new MODIFY COLUMN IF EXISTS "$v_source_col_ai" INT UNSIGNED NOT NULL DEFAULT '0' COMMENT 'colonne id sans auto_increment pour synchro depuis une table avec id auto-incrémenté' ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
  fi

  if [[ $v_nb_col_geometry -gt "0" ]]
  then

    # add a text columm in the target table to receive the converted to text geometric content
    v_sql="ALTER TABLE "$v_target_db"."$v_target_tab"_new ADD COLUMN IF NOT EXISTS "$v_target_col_text_geo" TEXT NOT NULL DEFAULT '' AFTER "$v_source_col_geometry" ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -D$v_target_db -v -e"${v_sql}"

    v_quote="\'\"\'"
    # modify the geometry column DEFAULT to back from text to geometry
    v_sql="ALTER TABLE "$v_target_db"."$v_target_tab"_new MODIFY COLUMN IF EXISTS "$v_source_col_geometry" GEOMETRY NOT NULL DEFAULT GeomFromText(CONCAT("${v_quote}", "$v_target_col_text_geo", "${v_quote}")) AFTER "$v_target_col_text_geo" ; "
    echo $v_sql
    mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -D$v_target_db -v -e"${v_sql}"

    v_search="ST_AsText("$v_source_col_geometry")"
    v_sub="${v_target_col_text_geo}"
    echo "-- substitution de '"$v_search"' par '"$v_sub"' dans v_liste_col_geo_in"
    v_liste_col_geo_in=$(sed "s/$v_search/$v_sub/g" <<< $v_liste_col_geo_out)

    # LOAD DATA LOCAL INFILE GEOMETRY
    echo "-- GEOMETRY - INFILE liste colonnes : "$v_liste_col_geo_in
    echo "-- LOAD DATA LOCAL INFILE - GEOMETRY"
    v_sql="load data local infile '"$v_path_csv"/"$v_output_file"' into table "$v_target_tab"_new character set utf8 fields terminated by '\t' lines terminated by '\n' ("$v_liste_col_geo_in") ; "
    echo "-- "$v_sql
    mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -e"${v_sql}" $v_target_db > $v_path_log"/cpimport.log"

  elif [[ $v_nb_col_ai_idb -gt "0" ]]
  then
    echo "-- AUTO_INCREMENT - INFILE liste colonnes : "$v_source_col_list
    echo "-- LOAD DATA LOCAL IN FILE - AUTO_INCREMENT"
    echo "-- "$v_sql
    v_sql="load data local infile '"$v_path_csv"/"$v_output_file"' into table "$v_target_tab"_new character set utf8 fields terminated by '\t' lines terminated by '\n' ("$v_souce_col_list") ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -e"${v_sql}" $v_target_db > $v_path_log"/cpimport.log"

  else

    # LOAD DATA LOCAL INFILE STANDARD
    echo "-- stndard - INFILE liste colonnes : "$v_source_col_list
    echo "-- LOAD DATA LOCAL IN FILE - standard"
    echo "-- "$v_sql
    v_sql="load data local infile '"$v_path_csv"/"$v_output_file"' into table "$v_target_tab"_new character set utf8 fields terminated by '\t' lines terminated by '\n' ("$v_souce_col_list") ; "
    mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -e"${v_sql}" $v_target_db > $v_path_log"/cpimport.log"

  fi

  # remove auto_increment property from the id_ai column
  if [[ $v_nb_col_ai_idb -gt "0" ]]
  then
    v_sql="ALTER TABLE  "$v_target_db"."$v_target_tab"_new MODIFY COLUMN IF EXISTS "$v_source_col_ai" INT UNSIGNED ZEROFILL NOT NULL AUTO_INCREMENT COMMENT 'colonne id avec auto_increment après synchro' ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db

    # set the auto_increment value on target table = same value as source table
    v_sql="ALTER TABLE "$v_source_tab"_new AUTO_INCREMENT="$v_source_max_ai" ; "
    v_opt="--verbose"
    v_db="target"
    f_mdb_sql_query_exec $v_opt "${v_sql}" $v_db
  fi

  # remove the no more needed geometry text column
  # if [[ $v_nb_col_geometry -gt "0" ]]
  # then
  #   v_sql="ALTER TABLE "$v_target_db"."$v_target_tab"_new DROP COLUMN IF EXISTS "$v_target_col_text_geo" ; "
  #   mariadb -A -u$v_usr -p$v_pwd -h$v_target_serv_host_name -D$v_target_db -v -e"${v_sql}"
  # fi
fi

# end upload date-time
end_time=$(date +%F' '%T'.'%N)

echo "-- traitement du fichier log"
# double quote deletion from the log file to prevent field delimiter error when the log content will be uploaded in the sync log table
sed -e 's/"//g' $v_path_log"/"cpimport.log > $v_path_log"/"log_thg.log
# back-slash and dash deletion from the log file
sed -i 's/\\//g;s/\-//g' $v_path_log"/"log_thg.log
# keep only the 100 first rows from the log file to be inserted in the sync log table
sed -i '101,$d' $v_path_log"/"log_thg.log
v_cpimport_log=$(cat $v_path_log"/"log_thg.log)

echo "-- suppression des fichiers SED"
rm -f sed*
rm -f log_thg.log

echo "-- calcul nb lignes table source"
# target table nb of rows to be compared with csv source file  nb of rows to verify upload int
v_sql="select count(*) from "$v_source_tab" ; "
v_opt="--quick --skip-column-names"
v_db="source"
v_nb_rows_source_tab=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)

echo "-- calcul nb lignes table ²cible"
# target table nb of rows to be compared with csv source file  nb of rows to verify upload int
v_sql="select count(*) from "$v_target_tab"_new ; "
v_opt="--quick --skip-column-names"
v_db="target"
v_nb_rows_target_tab=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)

echo ""
echo "-- fin chargement table cible  : "$v_target_db"."$v_target_tab"_new "
echo "-- nb lignes de la table cible : "$v_nb_rows_target_tab
echo "-- nom bdd et table source     : "$v_source_db"."$v_source_tab
echo "-- nb lignes table source      : "$v_nb_rows_source_tab

if [[ $v_nb_rows_target_tab -eq $v_nb_rows_source_tab ]]
then
  # echo ""
  echo "-- statut chargemt table cible : OK"
  # echo ""
  v_sql="drop table if exists "$v_target_tab"_old ; "
  v_opt="--verbose"
  v_db="target"
  f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db

  v_sql="rename table if exists "$v_target_tab" to "$v_target_tab"_old ; "
  v_opt="--verbose"
  v_db="target"
  f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db

  v_sql="rename table if exists "$v_target_tab"_new to "$v_target_tab" ; "
  v_opt="--verbose"
  v_db="target"
  f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db

  v_statut="OK"
  v_struct_tab_source_vs_target="IDENTIQUE"
  v_warning_msg=""
  v_error_msg=""
else
  # echo ""
  echo "-- statut chargement table cible :  ERREUR"
  # echo ""

  v_statut="ERREUR"
  f_aff_erreur "chargement table source "$v_target_tab

fi

v_maj_booleen_tab_req=", b_done = 1, b_restart = 0, b_force = 0"
f_journalisation

if [[ $v_process == "REQ" ]]
then
  v_sql="SELECT demand_dt, duration FROM _serv_transfert._transf_tab_serv_to_serv_request_idb WHERE id_ai = "$v_id_ai" ; "
  v_opt="--quick --skip-column-names"
  v_db="target"
  v_res=$(f_mdb_sql_query_exec "${v_opt}" "${v_sql}" $v_db)
  v_demande=$(awk -F'\t' '{ print $1 }' <<< $v_res)
  v_duree=$(awk -F'\t' '{ print $2 }' <<< $v_res)

  v_body="Bonjour,

SOURCE   - serveur : "$v_source_serv_name_upper"  - bdd : "$v_source_db" - table : "$v_source_tab"
CIBLE    - serveur : "$v_target_serv_name_upper" - bdd : "$v_target_db"  - table : "$v_target_tab"

horodatage demande : "$v_demande"

début exécution    : "$start_time"
fin exécution      : "$end_time"
durée transfert    : "$v_duree"

statut transfert   : "$v_statut"
Lignes transférées : "$v_nb_rows_target_tab" (vs. "$v_nb_rows_source_tab" lignes dans la table source)

Bien cordialement,
  "
  v_subject="TRANSFERT TABLE DE SERVEUR A SERVEUR"
  v_email_to=$v_email_demandeur
  v_email_cc="gerardin.thierry@gmail.com"

  f_envoi_email
fi
echo ""
