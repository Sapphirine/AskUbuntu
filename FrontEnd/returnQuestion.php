<?php
/*
####################################################
# Columbia University
# Department of Electrical Engineering
# Big Data - Final Project
# Spring 2015
# Andre Cunha
####################################################
*/
	header( 'Cache-Control: no-cache' );
	header( 'Content-type: application/xml; charset="utf-8"', true );

function Exec_BD($query,$dbase){
	$host = 127.0.0.1;
	$login = 'root';
	$password = 'password';
	
	$link = mysql_connect($host,$login,$password) or die(mysql_error());
	
	mysql_select_db("$dbase") or die(mysql_error());
	$result = mysql_query($query) or die(mysql_error()." ".$query);
	mysql_close($link);
	return $result;
}

	$dbase = 'AskUbuntu';
	$keyword = $_REQUEST['keyword'];
	
	$output = array();
	
	# Enter the number of recommendations
	$recommendations = 1
	
	for ($i= 1; $i <= $recommendations; $i++){
	  $sql = "SELECT id, name, rate, title
                FROM AskUbuntu WHERE name = '$keyword'";
                
           $res = Exec_BD($sql,$dbase);
           
           while ( $row = mysql_fetch_assoc( $res ) ) {
                $output = array(
                        'id'  => $row['id'],
                        'title' => $row['tile'],
                        'rate' => $row['rate']
                );
            }
           
	}
	
	echo( json_encode( $output ) );
	
?>