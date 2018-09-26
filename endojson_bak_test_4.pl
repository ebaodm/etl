use lib '/ccicall/dc/script/Public';
use MyTools;
use JSON;
use DBI;
use Data::Dumper;
use Time::HiRes qw/time/;
use Scalar::Util qw(reftype);
use threads;
$ENV{"NLS_DATE_FORMAT"}="YYYY-MM-DD HH24:MI:SS";
$ENV{"NLS_LANG"}="AMERICAN_AMERICA.AL32UTF8";
my $conncfg="/ccicall/dc/script/config/conn.cfg";

my $etlid=0;
#productid loopid为入参
if(!-f $conncfg){
	&writedblog($etlid,"","","E","配置文件不存/ccicall/dc/script/config/conn.cfg",&currenttime,&currenttime,"");
	exit 1;
}
if(@ARGV != 2){
	&writedblog($etlid,"","","E","参数错误",&currenttime,&currenttime,"");
	exit 1;
}
my ($productid,$loopid)=@ARGV;

my ($json,$relation);
my ($pkcol,$fkcol)=("DC_PK","DC_FK");#配置表
my $starttime=&currenttime;
open(CFG,$conncfg);
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
my @lvl;
my ($conf,$codetb,$codetblist,$maptype,$mapid,$str2num,$mappk,$mapobj,$mapeleid,$dateformat,$mapbizobj,$mappk,$compendono,$compendotrail,$endosubcfg);

my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
#DD产品树
my $sth=$dbh->prepare("select T.LV,
       T.CHILDTYPE,
       T.MODELNAME,
       T.FIELDNAME,
       T.RELATIONNAME,
       T.PARENTTYPE,
       T.CODETABLEID,
       T.PARENTMODEL,
       T.CHILDPATH,
       T.PARENTPATH,
       regexp_replace(NVL(A.TABLENAME,T.TABLENAME), '^T_', 'DM_') TABLENAME,
       NVL(A.COLUMNNAME,T.COLUMNNAME)
  from dc_json_config t
  LEFT join dc_json_column_map a
  on t.modelname=a.modelname
  and a.columnname=t.fieldname
  and a.isvalid='1'
	WHERE T.PRODUCTID = '".$productid."'
	and t.modelname not in ('PolicyChargeableClause','PolicyNonChargeableClause')
  AND NVL(A.COLUMNNAME,T.COLUMNNAME) IS NOT NULL
  AND NVL(A.TABLENAME,T.TABLENAME) IS NOT NULL
  AND NVL(A.COLUMNNAME,T.COLUMNNAME) NOT IN ('T_PA_PL_POLICY_ELEMENT','T_PA_POLICY_ELEMENT')
	AND NOT EXISTS(SELECT 1 FROM dc_invalid_column B
	WHERE T.TABLENAME=B.TABLENAME
	AND T.COLUMNNAME=B.COLUMNNAME)
	and not exists(select 1 from dc_invalid_table c
	where t.tablename=c.tablename)");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&writedblog($etlid,"","","E","dc_json_config配置不存在",&currenttime,&currenttime,"");
	exit 1;
}
#组装产品树配置
while(my @resrow=$sth->fetchrow_array){
	push @lvl,$resrow[0];
	#节点对应的表，对应的字段，对应的属性
	$conf->{$resrow[0]}->{$resrow[8]}->{"table"}->{$resrow[10]}->{"col"}->{$resrow[11]}->{"attr"}=$resrow[3];
	#对象属性对应的码表
	$conf->{$resrow[0]}->{$resrow[8]}->{"codetable"}->{$resrow[3]}=$resrow[6]  if defined $resrow[6];
	#对象间关系名
	$conf->{$resrow[0]}->{$resrow[8]}->{'relation'}=$resrow[4] if defined $resrow[4];
	#当前节点的父节点
	$conf->{$resrow[0]}->{$resrow[8]}->{'parent'}=$resrow[9] if defined $resrow[9];
	#初始化入参保单
	@{$conf->{$resrow[0]}->{$resrow[8]}->{'pkval'}}=@pk if $resrow[0] == 1;
	#主外键字段
	$conf->{$resrow[0]}->{$resrow[8]}->{'pkcol'}=$pkcol;
	$conf->{$resrow[0]}->{$resrow[8]}->{'fkcol'}=$fkcol;
	#数据库范围的哈希全部变为大写，需要映射回DD的驼峰
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($resrow[3])}=$resrow[3];
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($pkcol)}=$pkcol;
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($fkcol)}=$fkcol;
	#对象对应的模型
	$conf->{$resrow[0]}->{$resrow[8]}->{'modelname'}=$resrow[2];
	#对象间关系
	$relation->{$resrow[8]}->{$resrow[9]}=$resrow[4] if defined $resrow[4];
	#码表清单
	$codetblist->{$resrow[6]}="" if defined $resrow[6];
	#节点对应的当前对象的类型，初始化不考虑同一层级对应多个对象
	$maptype->{$resrow[8]}=$resrow[1] if ! exists $maptype->{$resrow[8]};
}
$sth->finish;
$dbh->disconnect;

#endosubcfg
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select A.TYPE,B.FIELD_NAME,regexp_replace(B.TABLENAME,'^T_','DM_'),B.COLUMNNAME,B.CODETABLEID
  from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
   and a.modelname = 'Endorsement'
   and a.objectcode<>'Endorsement'
   AND B.COLUMNNAME IS NOT NULL
   AND B.TABLENAME IS NOT NULL
   AND B.TABLENAME<>'T_PA_EDS_ENDORSEMENT'
   and not exists(select 1 from dc_invalid_column c where b.TABLENAME=c.TABLENAME and b.COLUMNNAME=c.COLUMNNAME)");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
my @endosubcdlist;
while(my @resrow=$sth->fetchrow_array){
	$endosubcfg->{$resrow[0]}->{"table"}->{$resrow[2]}->{"col"}->{$resrow[3]}->{"attr"}=$resrow[1];
	$endosubcfg->{$resrow[0]}->{"attrmap"}->{uc($resrow[1])}=$resrow[1];
	$codetblist->{$resrow[4]}="" if defined $resrow[4];
	$endosubcfg->{$resrow[0]}->{"codetable"}->{$resrow[1]}=$resrow[4] if defined $resrow[4];
}
$sth->finish();
$dbh->disconnect();

$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
#DD产品树
$sth=$dbh->prepare("select distinct codetableid
  from dc_admin.product_field_attr_tmp a, dc_admin.product_obj_tmp b
 where a.pk = b.pk
   and b.productid = '-1'
   and a.codetableid is not null");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
while(my @resrow=$sth->fetchrow_array){
	$codetblist->{$resrow[0]}="";
}
$sth->finish;
$dbh->disconnect;

#code映射配置
if(keys%{$codetblist}>0){
	my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
	my $codesql="select a.codetableid, a.srccode, a.code from dc_code_mapping a where a.codetableid in (";
	foreach my $codetableid (keys%{$codetblist}){
		$codesql.=$codetableid.",";
	}
	$codesql=~s/\,$//;
	$codesql.=")";
	$sth=$dbh->prepare($codesql);
	if(DBI->err){
		 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
		exit 1;
	}
	$sth->execute();
	if(DBI->err){
		 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
		exit 1;
	}
	while(my @resrow=$sth->fetchrow_array){
		$codetb->{$resrow[0]}->{$resrow[1]}=$resrow[2];
	}
	$sth->finish();
	$dbh->disconnect();
}

#数字类型转换配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select distinct a.modelname,b.field_name
 from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
 and a.productid in ( '".$productid."','-1')
 and b.datatype in ('INTEGER', 'DOUBLE')");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$str2num->{$resrow[0]}->{$resrow[1]}="";
}
$sth->finish();
$dbh->disconnect();

#日期类型转换配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select distinct a.modelname,b.field_name
 from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
 and a.productid in ('".$productid."','-1')
 and b.datatype in ('DATE')");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$dateformat->{$resrow[0]}->{$resrow[1]}="";
}
$sth->finish();
$dbh->disconnect();
=pod
#objectcode映射配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select a.modelname,
 a.objectcode,
 a.elementid,
 a.elementcode,
 a.pk,
 a.type,
 a.oldobjcode,
 a.attr
 from dc_obj_map_config a
 where a.productid = '".$productid."'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"objcode"}=$resrow[1];
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"elementcode"}=$resrow[3];
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"type"}=$resrow[5];
}
$sth->finish();
$dbh->disconnect();
=cut
#businessobjid映射配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("SELECT distinct a.parenttype||a.type, a.pk, a.objectcode
  from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
   and a.productid in ('".$productid."','-1')
   and b.field_name = 'BusinessObjectId'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&writedblog($etlid,"",$productid,"E","product_obj/product_field_attr配置不存在",$starttime,$starttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mapbizobj->{$resrow[0]}->{"id"}=$resrow[1];
	$mapbizobj->{$resrow[0]}->{"code"}=$resrow[2];
}
$sth->finish();
$dbh->disconnect();
#层级去重排序
my %count;
@lvl=grep { ++$count{ $_ } < 2; } @lvl;
undef %count;
@lvl=sort @lvl;

#@pk映射配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select a.current_table,a.attr_name from dc_id_mapping_config a where a.key_flag='P'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	return 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mappk->{$resrow[0]}=$resrow[1];
}
$sth->finish();
$dbh->disconnect();

#pd element
my $pdele;
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select a.type from product_obj_tmp a where a.elementid is not null and a.productid='".$productid."'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$pdele->{$resrow[0]}="";
}
$sth->finish();
$dbh->disconnect();


#根据产品树层级从根到叶，动态生成SQL，将数据库中数据放入哈希
#轮循产品层级
foreach my $lv (@lvl){
	#轮循产品节点
	foreach my $path (keys%{$conf->{$lv}}){
		my $alltmpdata;
		my $fulltmpdata;
		#轮循同一节点对应的多个表
		foreach my $tablen (keys%{$conf->{$lv}->{$path}->{"table"}}){
			#根据DD拼接动态SQL字段
			my $sql="select /*+parallel($tablen,4)*/";
			foreach my $col (keys%{$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}}){
				$sql.=$col." as ".$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}->{$col}->{"attr"}.",";
			}
			#关联主键驱动表
			if($lv ==1){
				#$sql.=" ".$pkcol."||endo_no as $pkcol,endo_no as $fkcol from dc_inc2.".$tablen." inner join dc_json_driver_endo_$loopid on ".$conf->{$lv}->{$path}->{"pkcol"}."=driver_key where endo_no<>'0'" ;	
				$sql.=" ".$pkcol."||endo_no as $pkcol,endo_no as $fkcol,endo_no,edit_flag from dc_inc2.".$tablen." inner join dc_json_driver_endo_$loopid on ".$conf->{$lv}->{$path}->{"pkcol"}."=driver_key" ;	
			}else{
				#$sql.=$pkcol."||endo_no as $pkcol,".$fkcol."||endo_no as $fkcol from dc_inc2.".$tablen." inner join (select distinct driver_key from dc_json_driver_endo_$loopid where lv=".($lv-1).") x on ".$conf->{$lv}->{$path}->{"fkcol"}."=x.driver_key where endo_no<>'0'";
				$sql.=$pkcol."||endo_no as $pkcol,".$fkcol."||endo_no as $fkcol,endo_no,edit_flag from dc_inc2.".$tablen." inner join (select distinct driver_key from dc_json_driver_endo_$loopid where lv=".($lv-1).") x on ".$conf->{$lv}->{$path}->{"fkcol"}."=x.driver_key";
			}
			$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},{'LongReadLen'=>60000});
			my $datasth=$dbh->prepare($sql);
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
				exit 1;
			}
			$datasth->execute();
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
				exit 1;
			}
			#以DC_PK字段为哈希键获取数据库哈希
			my $data=$datasth->fetchall_hashref($conf->{$lv}->{$path}->{"pkcol"});
			if($lv!=1){
				my $sthkey=$dbh->prepare("insert into dc_json_driver_endo_$loopid (driver_key,lv) select DISTINCT dc_pk,".$lv." from dc_inc2.$tablen a,dc_json_driver_endo_$loopid b where a.dc_fk=b.driver_key and b.lv=$lv-1 and not exists(select 1 from dc_json_driver_endo_$loopid c where dc_pk=c.driver_key and c.lv=$lv)");
				$sthkey->execute;
				$sthkey->finish;
			}else{
				#my $sthkey=$dbh->prepare("create table dc_json_driver_enno_$loopid as select ENDO_NO as driver_key,1 as lv from dc_inc2.".$tablen." a inner join dc_json_driver_endo_$loopid b on a.dc_fk=b.driver_key and b.lv=1 AND A.ENDO_NO<>'0'");
				my $sthkey=$dbh->prepare("create table dc_json_driver_enno_$loopid as select cast(ENDO_NO as varchar2(2000)) as driver_key,cast(1 as number(3)) as lv from dc_inc2.".$tablen." a inner join dc_json_driver_endo_$loopid b on a.dc_fk=b.driver_key and b.lv=1 AND A.ENDO_NO<>'0'");
				$sthkey->execute;
				exit 0 if $dbh->rows ==0;
				$sthkey->finish;
				$sthkey=$dbh->prepare("select a.new_policyno,
       		a.new_endorseno,
      		a.endor_sort
  				from dc_inc2.endorse_trail a
 					inner join dc_json_driver_endo_$loopid b
 					on b.driver_key = a.policyno");
 				$sthkey->execute;
 				while(my @row=$sthkey->fetchrow_array){
 					$compendono->{$row[1]}=$row[2];
 					$compendotrail->{$row[2]}->{$row[1]}=$row[1];
 				}
 				$sthkey->finish;
			}
			$dbh->disconnect;
			#轮循数据行
			foreach my $datapk (keys%{$data}){
				my $tmpdata;
				#轮循数据行的字段
				foreach my $attr(keys%{$data->{$datapk}}){
					$tmpdata->{"\@dcpk"}=$data->{$datapk}->{"DC_PK"};
					#非空判断，空值丢弃
					if( defined $data->{$datapk}->{$attr} and $data->{$datapk}->{$attr} ne "NULL"){
						#codemapping
						#代码字段映射处理
						if(exists($conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
							if(exists($codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}})){
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}};
							}else{
								#delete $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
								#&writecdmaplog($productid,$lv,$path,$attr,$datapk,$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}},$data->{$datapk}->{$attr});
							}
						#非代码字段处理
						}else{
							$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr} if exists($conf->{$lv}->{$path}->{"attrmapping"}->{$attr});
							#string转number
							if(exists($str2num->{$conf->{$lv}->{$path}->{'modelname'}}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								if($tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~/^\./){
									$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}='0'.$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								}
								"" if $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}} ==0;
							}
							#date格式化
							if(exists($dateformat->{$conf->{$lv}->{$path}->{'modelname'}}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~s/\s/T/g;
							}
						}
					}
				}
				$tmpdata->{'@dctype'}=$path;
				$tmpdata->{"ENDO_NO"}=$data->{$datapk}->{"ENDO_NO"};
				$tmpdata->{"EDIT_FLAG"}=$data->{$datapk}->{"EDIT_FLAG"};
=pod
				#elementcodemapping
				if(exists($mapobj->{$conf->{$lv}->{$path}->{'modelname'}}) and exists($tmpdata->{'ProductElementCode'})){
					$tmpdata->{'@type'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"type"};
					$tmpdata->{'ProductElementCode'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"elementcode"};
				}
=cut
				if(exists($tmpdata->{"ProductElementCode"})){
					$tmpdata->{'@type'}=$conf->{$lv}->{$path}->{'modelname'}."-".$tmpdata->{"ProductElementCode"}  if $conf->{$lv}->{$path}->{'modelname'} ne "PolicyForm";
					$tmpdata->{"ProductElementCode"}=$tmpdata->{"ProductElementCode"} if exists($pdele->{$tmpdata->{'@type'}});
					delete $tmpdata->{"ProductElementCode"}  if (! exists($pdele->{$tmpdata->{'@type'}})) and $conf->{$lv}->{$path}->{'modelname'} ne "PolicyForm";
				}
				#tempdata
				if(exists($tmpdata->{"UpdateTime"}) or exists($tmpdata->{"InsertTime"})){
					$tmpdata->{"TempData"}->{"InsertTime"}=$tmpdata->{"InsertTime"};
					$tmpdata->{"TempData"}->{"UpdateTime"}=$tmpdata->{"UpdateTime"};
					$tmpdata->{"TempData"}->{"InsertTime"}=~s/\s/T/g;
					$tmpdata->{"TempData"}->{"UpdateTime"}=~s/\s/T/g;
					delete $tmpdata->{"InsertTime"};
					delete $tmpdata->{"UpdateTime"};
				}
				#根据配置设置对象的主键@pk
				if(defined $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}}){
					if($tmpdata->{'EDIT_FLAG'} eq "I"){
						$tmpdata->{'OldPrimaryKey'}=$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
						#$tmpdata->{'PolicyElementId'}=$tmpdata->{'OldPrimaryKey'}  if $conf->{$lv}->{$path}->{"modelname"} eq "PolicyRisk";
						delete $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
					}else{
						$tmpdata->{'@pk'}=$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
						#$tmpdata->{'PolicyElementId'}=$tmpdata->{'@pk'} if $conf->{$lv}->{$path}->{"modelname"} eq "PolicyRisk";
						if ($tmpdata->{"EDIT_FLAG"} eq "U") {
							$tmpdata->{'PolicyStatus'}='2';
						}elsif($tmpdata->{"EDIT_FLAG"} eq "D"){
							$tmpdata->{'PolicyStatus'}='3';
						}
					}
					#$compendotrail->{$compendono->{$tmpdata->{"ENDO_NO"}}}->{"pk"}->{$tmpdata->{'@pk'}}=$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}};
					#$tmpdata->{"OldPrimaryKey"}=$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
					#"" if $tmpdata->{'OldPrimaryKey'}==0;
					#delete $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}} if $tablen ne "DM_PA_PL_POLICY";
				}
				push @{$conf->{$lv}->{$path}->{"pkval"}},$data->{$datapk}->{$conf->{$lv}->{$path}->{"pkcol"}} if $lv != 1;
				#将多个表放入对应一个对象的数据放入同一对象
				$tmpdata->{"ProductId"}=$productid if $conf->{$lv}->{$path}->{"modelname"} eq "Policy" or $conf->{$lv}->{$path}->{"modelname"} eq "CustomerDeclaration";
				foreach my $attrtmp(keys%{$tmpdata}){
					$fulltmpdata->{$datapk}->{$attrtmp}=$tmpdata->{$attrtmp};
				}
				undef $tmpdata;
			}
		}
		#将多行数据放入一个对象
		foreach my $datapk(keys%{$fulltmpdata}){
			#区分新增 修改 删除
			#if(not exists($compendotrail->{($compendono->{$fulltmpdata->{$datapk}->{"ENDO_NO"}}-1)}->{"pk"}->{$fulltmpdata->{$datapk}->{'@pk'}}) and exists($fulltmpdata->{$datapk}->{'@pk'})){
			#	$fulltmpdata->{$datapk}->{"OldPrimaryKey"}=$fulltmpdata->{$datapk}->{'@pk'};
			#	delete $fulltmpdata->{$datapk}->{$compendotrail->{$compendono->{$fulltmpdata->{$datapk}->{"ENDO_NO"}}}->{"pk"}->{$fulltmpdata->{$datapk}->{'@pk'}}};
			#	delete $fulltmpdata->{$datapk}->{'@pk'};
			#}
			#if(exists($compendotrail->{($compendono->{$fulltmpdata->{$datapk}->{"ENDO_NO"}}+1)}) and not exists($compendotrail->{($compendono->{$fulltmpdata->{$datapk}->{"ENDO_NO"}}+1)}->{"pk"}->{$fulltmpdata->{$datapk}->{'@pk'}})){
			#	my $delele=$fulltmpdata->{$datapk};
			#	$delele->{"PolicyStatus"}=3;
			#	$delele->{$fkcol}=$fulltmpdata->{$compendotrail->{($compendono->{$fulltmpdata->{$datapk}->{"ENDO_NO"}}+1)}->{"endono"}}->{$fkcol};
			#	push @{$alltmpdata},$delele;
			#}
			delete $fulltmpdata->{$datapk}->{"ENDO_NO"};
			delete $fulltmpdata->{$datapk}->{"EDIT_FLAG"};
			push @{$alltmpdata},$fulltmpdata->{$datapk};
		}
		undef $fulltmpdata;
		#主键驱动去重
		undef %count;
		@{$conf->{$lv}->{$path}->{"pkval"}}=grep { ++$count{ $_ } < 2; } @{$conf->{$lv}->{$path}->{"pkval"}};
		#放入json对象
		push @{$json->{$path}},@{$alltmpdata};
		undef $alltmpdata;
=pod
		#将当前层级的主键作为下一层级的外键
		foreach my $childtype (keys%{$conf->{$lv+1}}){
			if($conf->{$lv+1}->{$childtype}->{'parent'} eq $path and $lv+1 <= $lvl[-1]){
				@{$conf->{$lv+1}->{$childtype}->{'fkval'}}=@{$conf->{$lv}->{$path}->{"pkval"}};
			}
		}
=cut
	}
}
#open(FI,">>/datafile/debugcomp.txt");
#print FI encode_json($compendotrail)."\n";
#print FI encode_json($compendono);
#根据对象间关系组装出层级关系的JSON
#轮循关系配置
foreach my $type (keys%{$relation}){
	foreach my $parenttype (keys%{$relation->{$type}}){
		#轮循数据行
		for(my $parentidx=0;$parentidx < @{$json->{$parenttype}};$parentidx++){
			for(my $childidx=0;$childidx < @{$json->{$type}};$childidx++){
				#父节点的pk等于子节点的fk，那么可以将子对象挂上
				if($json->{$parenttype}[$parentidx]->{"\@dcpk"} eq $json->{$type}[$childidx]->{$fkcol} and $parenttype eq $json->{$parenttype}[$parentidx]->{'@dctype'}){
					#productid映射
					$json->{$type}[$childidx]->{'ProductId'}=$productid if exists($json->{$type}[$childidx]->{'ProductId'});
					"" if $json->{$type}[$childidx]->{'ProductId'} == 0;
					#根据产品树节点及对象映射配置确定对象类型
					$json->{$type}[$childidx]->{'@type'}=$maptype->{$type} if exists($maptype->{$json->{$type}[$childidx]->{'@dctype'}}) and ! exists($json->{$type}[$childidx]->{'@type'});
					#next if ! defined $json->{$type}[$childidx]->{"\@type"};
					#ProductLobId api报多余字段，删除
					delete $json->{$type}[$childidx]->{"ProductLobId"} if exists $json->{$type}[$childidx]->{"ProductLobId"};
					push @{$json->{$parenttype}[$parentidx]->{$relation->{$type}->{$parenttype}}},$json->{$type}[$childidx];
				}
			}
		}
	}
}

#计算全路径获取productelementid
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
#对象全路径对应的businessobjectid/productelementid
$sth=$dbh->prepare("select a.parenttype || '/' || a.type, a.pk, a.elementid
  from product_obj_tmp a
 where a.productid = '".$productid."'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
my $mapeleid;
while(my @resrow=$sth->fetchrow_array){
	$mapeleid->{$resrow[0]}->{"objectid"}=$resrow[1];
	$mapeleid->{$resrow[0]}->{"elementid"}=$resrow[2] if defined $resrow[2];
}
$sth->finish;
$dbh->disconnect;

#set对象的businessobjectid/productelementid
sub setproductelement{
	my ($obj,$typepath,$mapeleid)=@_;
	my $type=reftype $obj;
	if($type eq "HASH"){
		$typepath.="/".$obj->{'@type'};
		if(exists($mapeleid->{$typepath})){
			$obj->{"BusinessObjectId"}=$mapeleid->{$typepath}->{"objectid"};
			"" if $obj->{"BusinessObjectId"}==0;
			if(exists($mapeleid->{$typepath}->{"elementid"})){
				$obj->{"ProductElementId"}=$mapeleid->{$typepath}->{"elementid"};
				"" if $obj->{"ProductElementId"}==0;
			}
		}
	}
	#删除DC技术字段
	foreach(@delcol){
		delete $obj->{$_} if exists($obj->{$_}) and $obj->{'@type'} ne "Policy-POLICY";
	}
	foreach my $key(keys%{$obj}){
		$type=reftype $obj->{$key};
		if($type eq "ARRAY"){
			cutarray($obj->{$key},$typepath,$mapeleid);
		}
		delete $obj->{$key} if $key eq "PolicyStatusAtChildTable";
		delete $obj->{$key} if ! defined $obj->{$key};
	}
	return 0;
}
sub cutarray{
	my ($obj,$typepath,$mapeleid)=@_;
	foreach my $ele(@{$obj}){
		setproductelement($ele,$typepath,$mapeleid);
	}
	return 0;
}
#DC技术字段
our @delcol=("DC_PK","DC_FK",'@dctype','@dcpk');
#轮循产品节点
foreach my $path(keys%{$json}){
	#POLICY已是完整的，删除非POLICY节点
	delete $json->{$path} and next if $path ne "/POLICY";
	#计算每个对象的全路径，调用set对象businessobjectid/productelementid函数
	for(my $count=0;$count<@{$json->{$path}};$count++){
		$json->{$path}[$count]->{'@type'}='Policy-POLICY';
		setproductelement($json->{$path}[$count],"",$mapeleid);
	}
}

$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},{'LongReadLen'=>90000});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select a.endotype,a.endoobjtype,objectid from dc_json_endo_obj_map a");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
my $endoobjmap;
my $endoobjid;
while(my @resrow=$sth->fetchrow_array){
	$endoobjmap->{$resrow[0]}=$resrow[1];
	$endoobjid->{$resrow[0]}=$resrow[2];
}
$sth->finish;
$dbh->disconnect;
#print $endoobjid."\n";

#批单对象
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},{'LongReadLen'=>90000});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select '/Endorsement'||a.parent,
			 a.parenttype,
       a.type,
       regexp_replace('/Endorsement'||a.parent,'/[^\\/]+\$',''),
       a.relationname,
       a.modelname,
       b.field_name,
       regexp_replace(b.tablename,'^T_','DM_'),
       b.columnname,
       b.codetableid,
       b.datatype
  from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
   and a.productid = '-1'
   and ('/Endorsement' || a.parent not like '%/PolicyList%' or a.type='Endorsement-Endorsement')
   and b.tablename is not null
   and b.columnname is not null
   AND NOT EXISTS(SELECT 1 FROM DC_INVALID_COLUMN C
   WHERE B.COLUMNNAME=C.COLUMNNAME
   AND b.tablename=C.TABLENAME)");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
my ($endocfg,$endocdtblist,$endorela,$endomaptype,$endojson);
my @endolvl;
while(my @resrow=$sth->fetchrow_array){
	my $typepathtmp=$resrow[0];
	$typepathtmp=~s/\///g;
	my $lvtmp=length($resrow[0])-length($typepathtmp);
	push @endolvl,$lvtmp;
	#节点对应的表，对应的字段，对应的属性
	$endocfg->{$lvtmp}->{$resrow[0]}->{"table"}->{$resrow[7]}->{"col"}->{$resrow[8]}->{"attr"}=$resrow[6];
	#对象属性对应的码表
	$endocfg->{$lvtmp}->{$resrow[0]}->{"codetable"}->{$resrow[6]}=$resrow[9]  if defined $resrow[9] and $resrow[6] ne 'EndoType';
	#对象间关系名
	$endocfg->{$lvtmp}->{$resrow[0]}->{'relation'}=$resrow[4] if defined $resrow[4];
	#当前节点的父节点
	$endocfg->{$lvtmp}->{$resrow[0]}->{'parent'}=$resrow[1] if defined $resrow[1];
	#初始化入参保单
	@{$endocfg->{$lvtmp}->{$resrow[0]}->{'pkval'}}=@pk if $resrow[0] == 1;
	#主外键字段
	$endocfg->{$lvtmp}->{$resrow[0]}->{'pkcol'}=$pkcol;
	$endocfg->{$lvtmp}->{$resrow[0]}->{'fkcol'}=$fkcol;
	#数据库范围的哈希全部变为大写，需要映射回DD的驼峰
	$endocfg->{$lvtmp}->{$resrow[0]}->{'attrmapping'}->{uc($resrow[6])}=$resrow[6];
	$endocfg->{$lvtmp}->{$resrow[0]}->{'attrmapping'}->{uc($pkcol)}=$pkcol;
	$endocfg->{$lvtmp}->{$resrow[0]}->{'attrmapping'}->{uc($fkcol)}=$fkcol;
	#对象对应的模型
	$endocfg->{$lvtmp}->{$resrow[0]}->{'modelname'}=$resrow[2];
	#对象间关系
	$endorela->{$resrow[0]}->{$resrow[3]}=$resrow[4] if defined $resrow[4];
	#码表清单
	$endocdtblist->{$resrow[9]}="" if defined $resrow[9] and $resrow[6] ne 'EndoType';
	#节点对应的当前对象的类型，初始化不考虑同一层级对应多个对象
	$endomaptype->{$resrow[0]}=$resrow[2] if ! exists $endomaptype->{$resrow[0]};
}
$sth->finish;
$dbh->disconnect;
my %count;
@endolvl=grep { ++$count{ $_ } < 2; } @endolvl;
undef %count;
@endolvl=sort @endolvl;
#open(DEBUG,">>./ENDOCFG.TXT");
#print DEBUG Dumper($endocfg);
#close(DEBUG);
foreach my $lv (@endolvl){
	#轮循产品节点
	foreach my $path (keys%{$endocfg->{$lv}}){
		my $alltmpdata;
		my $fulltmpdata;
		#轮循同一节点对应的多个表
		foreach my $tablen (keys%{$endocfg->{$lv}->{$path}->{"table"}}){
			#根据DD拼接动态SQL字段
			my $sql="select /*+parallel($tablen,4)*/";
			foreach my $col (keys%{$endocfg->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}}){
				$sql.=$col." as ".$endocfg->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}->{$col}->{"attr"}.",";
			}
			#关联主键驱动表
			if($lv ==1){
				$sql.=" ".$pkcol." as $pkcol,$fkcol from dc_inc2.".$tablen." inner join dc_json_driver_enno_$loopid on ".$endocfg->{$lv}->{$path}->{"pkcol"}."=driver_key" ;	
			}else{
				$sql.=$pkcol." as $pkcol,".$fkcol." as $fkcol from dc_inc2.".$tablen." inner join (select distinct driver_key from dc_json_driver_enno_$loopid where lv=".($lv-1).") x on ".$endocfg->{$lv}->{$path}->{"fkcol"}."=x.driver_key";
			}
			$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},{'LongReadLen'=>90000});
			#print $sql."\n-------------\n";
			my $datasth=$dbh->prepare($sql);
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
			}
			$datasth->execute();
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
			}
			#以DC_PK字段为哈希键获取数据库哈希
			my $data=$datasth->fetchall_hashref($endocfg->{$lv}->{$path}->{"pkcol"});
			if($lv!=1){
				my $sthkey=$dbh->prepare("insert into dc_json_driver_enno_$loopid (driver_key,lv) select DISTINCT dc_pk,".$lv." from dc_inc2.$tablen a,dc_json_driver_enno_$loopid b where a.dc_fk=b.driver_key and b.lv=$lv-1 and not exists(select 1 from dc_json_driver_enno_$loopid c where dc_pk=c.driver_key and c.lv=$lv)");
				$sthkey->execute;
				$sthkey->finish;
			}
			$dbh->disconnect;
			#轮循数据行
			foreach my $datapk (keys%{$data}){
				my $tmpdata;
				#轮循数据行的字段
				foreach my $attr(keys%{$data->{$datapk}}){
					$tmpdata->{"\@dcpk"}=$data->{$datapk}->{"DC_PK"};
					#非空判断，空值丢弃
					if( defined $data->{$datapk}->{$attr} and $data->{$datapk}->{$attr} ne 'NULL'){
						#codemapping
						#代码字段映射处理
						if(exists($endocfg->{$lv}->{$path}->{"codetable"}->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
							if(exists($codetb->{$endocfg->{$lv}->{$path}->{"codetable"}->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}})){
								$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$codetb->{$endocfg->{$lv}->{$path}->{"codetable"}->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}->{$data->{$datapk}->{$attr}}};
							}else{
								#delete $tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
								"";#&writecdmaplog($productid,$lv,$path,$attr,$datapk,$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}},$data->{$datapk}->{$attr});
							}
						#非代码字段处理
						}else{
							$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
							#string转number
							if(exists($str2num->{$endocfg->{$lv}->{$path}->{'modelname'}}->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								if($tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~/^\./){
									$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}='0'.$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								}
								"" if $tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}} ==0;
							}
							#date格式化
							if(exists($dateformat->{$endocfg->{$lv}->{$path}->{'modelname'}}->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~s/\s/T/g;
							}
						}
					}
				}
				$tmpdata->{'@dctype'}=$path;
				#elementcodemapping
				if(exists($mapobj->{$endocfg->{$lv}->{$path}->{'modelname'}}) and exists($tmpdata->{'ProductElementCode'})){
					$tmpdata->{'@type'}=$mapobj->{$endocfg->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"type"};
					$tmpdata->{'ProductElementCode'}=$mapobj->{$endocfg->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"elementcode"};
				}
				#tempdata
				if(exists($tmpdata->{"UpdateTime"}) or exists($tmpdata->{"InsertTime"})){
					$tmpdata->{"TempData"}->{"InsertTime"}=$tmpdata->{"InsertTime"};
					$tmpdata->{"TempData"}->{"UpdateTime"}=$tmpdata->{"UpdateTime"};
					$tmpdata->{"TempData"}->{"InsertTime"}=~s/\s/T/g;
					$tmpdata->{"TempData"}->{"UpdateTime"}=~s/\s/T/g;
					delete $tmpdata->{"InsertTime"};
					delete $tmpdata->{"UpdateTime"};
				}
				#根据配置设置对象的主键@pk
				if(defined $tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}}){
					$tmpdata->{'@pk'}=$tmpdata->{$endocfg->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
					"" if $tmpdata->{'@pk'}==0;
				}
				push @{$endocfg->{$lv}->{$path}->{"pkval"}},$data->{$datapk}->{$endocfg->{$lv}->{$path}->{"pkcol"}} if $lv != 1;
				#将多个表放入对应一个对象的数据放入同一对象
				foreach my $attrtmp(keys%{$tmpdata}){
					$fulltmpdata->{$datapk}->{$attrtmp}=$tmpdata->{$attrtmp};
				}
				undef $tmpdata;
			}
		}
		#将多行数据放入一个对象
		foreach my $datapk(keys%{$fulltmpdata}){
			push @{$alltmpdata},$fulltmpdata->{$datapk};
		}
		undef $fulltmpdata;
		#主键驱动去重
		undef %count;
		@{$endocfg->{$lv}->{$path}->{"pkval"}}=grep { ++$count{ $_ } < 2; } @{$endocfg->{$lv}->{$path}->{"pkval"}};
		#放入json对象
		push @{$endojson->{$path}},@{$alltmpdata};
		undef $alltmpdata;
		#将当前层级的主键作为下一层级的外键
		foreach my $childtype (keys%{$conf->{$lv+1}}){
			if($endocfg->{$lv+1}->{$childtype}->{'parent'} eq $path and $lv+1 <= $lvl[-1]){
				@{$endocfg->{$lv+1}->{$childtype}->{'fkval'}}=@{$endocfg->{$lv}->{$path}->{"pkval"}};
			}
		}
	}
}

foreach my $type (keys%{$endorela}){
	foreach my $parenttype (keys%{$endorela->{$type}}){
		#轮循数据行
		for(my $parentidx=0;$parentidx < @{$endojson->{$parenttype}};$parentidx++){
			for(my $childidx=0;$childidx < @{$endojson->{$type}};$childidx++){
				#父节点的pk等于子节点的fk，那么可以将子对象挂上
				if($endojson->{$parenttype}[$parentidx]->{"\@dcpk"} eq $endojson->{$type}[$childidx]->{$fkcol} and $parenttype eq $endojson->{$parenttype}[$parentidx]->{'@dctype'}){
					#productid映射
					$endojson->{$type}[$childidx]->{'ProductId'}=$productid if exists($endojson->{$type}[$childidx]->{'ProductId'});
					"" if $endojson->{$type}[$childidx]->{'ProductId'} == 0;
					#根据产品树节点及对象映射配置确定对象类型
					$endojson->{$type}[$childidx]->{'@type'}=$endomaptype->{$type} if exists($endomaptype->{$endojson->{$type}[$childidx]->{'@dctype'}}) and ! exists($endojson->{$type}[$childidx]->{'@type'});
					#next if ! defined $endojson->{$type}[$childidx]->{"\@type"};
					#ProductLobId api报多余字段，删除
					delete $endojson->{$type}[$childidx]->{"ProductLobId"} if exists $endojson->{$type}[$childidx]->{"ProductLobId"};
					delete $endojson->{$type}[$childidx]->{"DC_PK"} if exists $endojson->{$type}[$childidx]->{"DC_PK"};
					delete $endojson->{$type}[$childidx]->{'@dctype'} if exists $endojson->{$type}[$childidx]->{'@dctype'};
					delete $endojson->{$type}[$childidx]->{'@dcpk'} if exists $endojson->{$type}[$childidx]->{'@dcpk'};
					delete $endojson->{$type}[$childidx]->{"DC_FK"} if exists $endojson->{$type}[$childidx]->{"DC_FK"};
					push @{$endojson->{$parenttype}[$parentidx]->{$endorela->{$type}->{$parenttype}}},$endojson->{$type}[$childidx];
				}
			}
		}
	}
}

for(my $policyidx;$policyidx<@{$json->{"/POLICY"}};$policyidx++){
	for(my $endoidx;$endoidx<@{$endojson->{"/Endorsement"}};$endoidx++){
		if($json->{"/POLICY"}[$policyidx]->{"DC_FK"} eq $endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}){
			#$json->{"/POLICY"}[$policyidx]->{"PolicyStatus"}=2;
			$endojson->{"/Endorsement"}[$endoidx]->{"NewPolicy"}=$json->{"/POLICY"}[$policyidx];
			$endojson->{"/Endorsement"}[$endoidx]->{"PolicyId"}=$json->{"/POLICY"}[$policyidx]->{"PolicyId"};
			delete $endojson->{"/Endorsement"}[$endoidx]->{'@dctype'};
			delete $endojson->{"/Endorsement"}[$endoidx]->{'@dcpk'};
			delete $endojson->{"/Endorsement"}[$endoidx]->{'DC_FK'};
			#endorsement type
			$endojson->{"/Endorsement"}[$endoidx]->{'@type'}=$endoobjmap->{$endojson->{"/Endorsement"}[$endoidx]->{"EndoType"}};
			$endojson->{"/Endorsement"}[$endoidx]->{'ProductId'}=$productid;
			$endojson->{"/Endorsement"}[$endoidx]->{'EndoStatus'}=300;#核批通过
			delete $endojson->{"/Endorsement"}[$endoidx]->{"NewPolicy"}->{'DC_PK'};
			delete $endojson->{"/Endorsement"}[$endoidx]->{"NewPolicy"}->{'@dctype'};
			delete $endojson->{"/Endorsement"}[$endoidx]->{"NewPolicy"}->{'@dcpk'};
			delete $endojson->{"/Endorsement"}[$endoidx]->{"NewPolicy"}->{'DC_FK'};
		}
	}
}

$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select a.driver_key, b.endor_sort,nvl(b.new_policyno,policyno)
  from dc_admin.dc_json_driver_enno_$loopid a
 inner join dc_inc2.endorse_trail b
    on a.driver_key = b.new_endorseno
 where a.lv = '1'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
my $endotrail;
my $endopolicy;
while(my @resrow=$sth->fetchrow_array){
	$endotrail->{$resrow[0]}=$resrow[1];
	$endopolicy->{$resrow[0]}=$resrow[2];
}
$sth->finish;
$dbh->disconnect;

my $endojsontrail;
for(my $endoidx;$endoidx<@{$endojson->{"/Endorsement"}};$endoidx++){
	#批改字表
	#$endosubcfg->{$resrow[0]}->{"table"}->{$resrow[2]}->{"col"}->{$resrow[3]}->{"attr"}=$resrow[1];
	#$endosubcfg->{$resrow[0]}->{"attrmap"}->{uc($resrow[1])}=$resrow[1];
	#$endosubcfg->{$resrow[0]}->{"codetable"}->{$resrow[1]}=$resrow[4] if defined $resrow[4];
	if(exists($endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}})){
		foreach my $tab(keys%{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"table"}}){
			my $endosubsql="select dc_pk,";
			foreach my $col(keys%{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"table"}->{$tab}->{"col"}}){
				$endosubsql.=$col." as ".$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"table"}->{$tab}->{"col"}->{$col}->{"attr"}.",";
			}
			$endosubsql=~s/\,$//;
			$endosubsql.=" from dc_inc2.".$tab." where dc_pk='".$endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}."'";
			#print $endosubsql."\n---------\n";
			my $endosubdbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
			my $endosubsth=$endosubdbh->prepare($endosubsql);
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
				$endosubdbh->do("drop table dc_json_driver_enno_$loopid");
				exit 1;
			}
			$endosubsth->execute;
			my $endosubdata=$endosubsth->fetchall_hashref("DC_PK");
			#$codetb->{$resrow[0]}->{$resrow[1]}=$resrow[2];
			foreach my $datapk(keys%{$endosubdata}){
				foreach my $attr(keys%{$endosubdata->{$datapk}}){
					next if $attr eq "DC_PK";
					if(! defined $endosubdata->{$datapk}->{$attr} or $endosubdata->{$datapk}->{$attr} eq "NULL"){
						next;
					}
					if(exists($endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"codetable"}->{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"attrmap"}->{$attr}})){
						$endojson->{"/Endorsement"}[$endoidx]->{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"attrmap"}->{$attr}}=$codetb->{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"codetable"}->{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"attrmap"}->{$attr}}}->{$endosubdata->{$datapk}->{$attr}};
					}else{
						$endojson->{"/Endorsement"}[$endoidx]->{$endosubcfg->{$endojson->{"/Endorsement"}[$endoidx]->{'@type'}}->{"attrmap"}->{$attr}}=$endosubdata->{$datapk}->{$attr};
					}
				}
			}
			$endosubsth->finish;
			$endosubdbh->disconnect;
		}
	}
	$endojsontrail->{$endopolicy->{$endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}}}->{$endotrail->{$endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}}}=$endojson->{"/Endorsement"}[$endoidx];
	delete $endojsontrail->{$endopolicy->{$endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}}}->{$endotrail->{$endojson->{"/Endorsement"}[$endoidx]->{"DC_PK"}}}->{"DC_PK"};
}
#print Dumper($endojsontrail);
$filepath=$conncfg{"jsonpath"}."/e_".$starttime."_".$productid."_".$loopid.".txt";
open(FI,">>$filepath");
foreach my $key(keys%{$endojsontrail}){
	print FI encode_json($endojsontrail->{$key})."\n";
}
close(FI);
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$dbh->do("drop table dc_json_driver_enno_$loopid");

if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$dbh->disconnect;
=pod
dc_json_config auto
dc_invalid_column auto
product_field_attr_tmp auto
product_obj_tmp auto
dc_code_mapping handle
dc_obj_map_config handle
dc_id_mapping_config handle
dc_invalid_table handle
dc_valid_productid handle
dc_invalid_column handle
=cut