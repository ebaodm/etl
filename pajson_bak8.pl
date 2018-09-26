use lib '/ccicall/dc/script/Public';
use MyTools;
use JSON;
use DBI;
use Data::Dumper;
use Time::HiRes qw/time/;
use Scalar::Util qw(reftype);
use threads;
$ENV{"NLS_DATE_FORMAT"}="YYYY-MM-DD HH24:MI:SS";
my $conncfg="/ccicall/dc/script/config/conn.cfg";

sub driverlog{
	my ($editflag,$drivertb,$filename)=@_;
	my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"}) or return {"errcode"=>DBI->err,"errmsg"=>DBI->errstr};
	if($editflag==1){
		$dbh->do("insert into dc_json_driver_log(driverkey,filename,errcode) select driver_key,'".$filename."',0 from ".$drivertb." where lv=1") or return {"errcode"=>DBI->err,"errmsg"=>DBI->errstr};
	}elsif($editflag==0){
		$dbh->do("insert into dc_json_driver_log(driverkey,errcode) select driver_key,1 from ".$drivertb." where lv=1") or return {"errcode"=>DBI->err,"errmsg"=>DBI->errstr};
	}
	$dbh->disconnect();
}


my $etlid=0;
#productid loopid为入参
if(!-f $conncfg){
	&writedblog($etlid,"","","E","配置文件不存/ccicall/dc/script/config/conn.cfg",&currenttime,&currenttime,"");
	exit 1;
}
if(@ARGV != 3 and @ARGV!=4){
	&writedblog($etlid,"","","E","参数错误",&currenttime,&currenttime,"");
	exit 1;
}
my $productid,$loopid,$tablespace,$exppath;
($productid,$loopid,$tablespace)=@ARGV if @ARGV ==3;
($productid,$loopid,$tablespace,$exppath)=@ARGV if @ARGV ==4;

my ($json,$relation);
my ($pkcol,$fkcol)=("DC_PK","DC_FK");#配置表
my $starttime=&currenttime;
open(CFG,$conncfg);
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
my @lvl;
my ($conf,$codetb,$codetblist,$maptype,$mapid,$str2num,$mappk,$mapobj,$mapeleid,$dateformat,$mapbizobj,$mappk,$noncode);

my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
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
  AND NVL(A.TABLENAME,T.TABLENAME) NOT IN ('T_PA_PL_POLICY_ELEMENT','T_PA_POLICY_ELEMENT')
  AND NOT EXISTS(SELECT 1 FROM dc_invalid_column B
  WHERE T.TABLENAME=B.TABLENAME
  AND T.COLUMNNAME=B.COLUMNNAME)
  and not exists(select 1 from dc_invalid_table c
  where t.tablename=c.tablename)");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&driverlog(1,"dc_json_driver_$loopid");
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
		&driverlog(1,"dc_json_driver_$loopid");
		&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
		exit 1;
	}
	$sth->execute();
	if(DBI->err){
		&driverlog(1,"dc_json_driver_$loopid");
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
 and a.productid = '".$productid."'
 and b.datatype in ('INTEGER', 'DOUBLE')");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
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
 and a.productid = '".$productid."'
 and b.datatype in ('DATE')");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$dateformat->{$resrow[0]}->{$resrow[1]}="";
}
$sth->finish();
$dbh->disconnect();

#pd element
my $pdele;
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select a.type from product_obj_tmp a where a.elementid is not null and a.productid='".$productid."'");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$pdele->{$resrow[0]}="";
}
$sth->finish();
$dbh->disconnect();

=pod
mapping中指定,不再需要
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
   and a.productid = '".$productid."'
   and b.field_name = 'BusinessObjectId'");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,"E","product_obj/product_field_attr配置不存在",$startime,$startime,"");
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
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select a.current_table,a.attr_name from dc_id_mapping_config a where key_flag='P'");
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	return 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mappk->{$resrow[0]}=$resrow[1];
}
$sth->finish();
$dbh->disconnect();
=pod
#不转码配置
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select modelname,fieldname from dc_json_noncodemapping");
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
	$noncode->{$resrow[0]}=$resrow[1];
}
$sth->finish();
$dbh->disconnect();
=cut
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
			my $sql="select /*+parallel($tablen,1)*/";
			foreach my $col (keys%{$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}}){
				$sql.=$col." as ".$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}->{$col}->{"attr"}.",";
			}
			
			#关联主键驱动表
			if($lv ==1){
				$sql.=" ".$pkcol." from $tablespace.".$tablen." inner join dc_json_driver_$loopid on ".$conf->{$lv}->{$path}->{"pkcol"}."=driver_key where endo_no='0'" ;
			}else{
				$sql.=$pkcol.",".$fkcol." from $tablespace.".$tablen." inner join (select distinct driver_key from dc_json_driver_$loopid where lv=".($lv-1).") x on ".$conf->{$lv}->{$path}->{"fkcol"}."=x.driver_key where endo_no='0'";
			}
			#print $sql."\n" if $path eq "/POLICY/PolicyCFeeList";
			$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"},{'LongReadLen'=>60000});
			my $datasth=$dbh->prepare($sql);
			if(DBI->err){
				&driverlog(1,"dc_json_driver_$loopid");
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
				exit 1;
			}
			$datasth->execute();
			if(DBI->err){
				&driverlog(1,"dc_json_driver_$loopid");
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
				exit 1;
			}
			#以DC_PK字段为哈希键获取数据库哈希
			my $data=$datasth->fetchall_hashref($conf->{$lv}->{$path}->{"pkcol"});
			#print Dumper($data)."||||||||||||||||\n" if $path eq "/POLICY/PolicyCFeeList";
			if($lv!=1){
				my $sthkey=$dbh->prepare("insert into dc_json_driver_$loopid (driver_key,lv) select DISTINCT dc_pk,".$lv." from $tablespace.$tablen a,dc_json_driver_$loopid b where a.dc_fk=b.driver_key and b.lv=$lv-1 and not exists(select 1 from dc_json_driver_$loopid c where dc_pk=c.driver_key and c.lv=$lv)");
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
						#if(exists($conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}} and not exists($noncode->{$conf->{$lv}->{$path}->{"modelname"}}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}))){
						if(exists($conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
							if(exists($codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}})){
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}};
								#print $data->{$datapk}->{$attr}."|".$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}."|".$codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}}."\n" if $attr eq "PREMIUMCURRENCYCODE" and $lv ==1;
							}else{
								#"";
								#delete $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
								#&writecdmaplog($productid,$lv,$path,$attr,$datapk,$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}},$data->{$datapk}->{$attr});
							}
						#非代码字段处理
						}else{
							$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
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
=pod
				#elementcodemapping
				if(exists($mapobj->{$conf->{$lv}->{$path}->{'modelname'}}) and exists($tmpdata->{'ProductElementCode'})){
					$tmpdata->{'@type'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"type"};
					$tmpdata->{'ProductElementCode'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"elementcode"};
				}
=cut
				if(exists($tmpdata->{"ProductElementCode"})){
					$tmpdata->{'@type'}=$conf->{$lv}->{$path}->{'modelname'}."-".$tmpdata->{"ProductElementCode"} if $conf->{$lv}->{$path}->{'modelname'} ne "PolicyForm";
					$tmpdata->{"ProductElementCode"}=$tmpdata->{"ProductElementCode"} if exists($pdele->{$tmpdata->{'@type'}});
					delete $tmpdata->{"ProductElementCode"} if (! exists($pdele->{$tmpdata->{'@type'}})) and $conf->{$lv}->{$path}->{'modelname'} ne "PolicyForm";
					#delete $tmpdata->{"ProductElementCode"};
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
				$tmpdata->{'@pk'}=$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}} if defined $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
				#print $path."\n" and next if ! defined $tmpdata->{'@pk'};
				"" if $tmpdata->{'@pk'}==0;
				$tmpdata->{"ProductId"}=$productid if $conf->{$lv}->{$path}->{"modelname"} eq "Policy" or $conf->{$lv}->{$path}->{"modelname"} eq "CustomerDeclaration";
				push @{$conf->{$lv}->{$path}->{"pkval"}},$data->{$datapk}->{$conf->{$lv}->{$path}->{"pkcol"}} if $lv != 1;
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
					print $type."\n" and next if ! defined $json->{$type}[$childidx]->{"\@type"};
					#ProductLobId api报多余字段，删除
					delete $json->{$type}[$childidx]->{"ProductLobId"} if exists $json->{$type}[$childidx]->{"ProductLobId"};
					$json->{$type}[$childidx]->{"PolicyStatus"}=2;
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
	&driverlog(1,"dc_json_driver_$loopid");
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$starttime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&driverlog(1,"dc_json_driver_$loopid");
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
		delete $obj->{$_} if exists($obj->{$_});
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

$filepath=$conncfg{"jsonpath"}."/".$starttime."_".$productid."_".$loopid.".txt" if ! defined $exppath;
$filepath=$exppath."/".$starttime."_".$productid."_".$loopid.".txt" if defined $exppath;
open(FI,">>".$filepath);
for(my $count=0; $count<@{$json->{'/POLICY'}};$count++){
	$json->{'/POLICY'}[$count]->{"PolicyStatus"}=2;
	my $jsstr=encode_json($json->{'/POLICY'}[$count]);
	print FI $jsstr."\n";
}
&driverlog(0,"dc_json_driver_$loopid",$filepath);
