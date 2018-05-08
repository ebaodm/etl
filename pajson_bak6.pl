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

my $etlid=0;
#productid loopidΪ���
if(!-f $conncfg){
	&writedblog($etlid,"","","E","�����ļ�����/ccicall/dc/script/config/conn.cfg",&currenttime,&currenttime,"");
	exit 1;
}
if(@ARGV != 2){
	&writedblog($etlid,"","","E","��������",&currenttime,&currenttime,"");
	exit 1;
}
my ($productid,$loopid)=@ARGV;

my ($json,$relation);
my ($pkcol,$fkcol)=("DC_PK","DC_FK");#���ñ�
my $starttime=&currenttime;
open(CFG,$conncfg);
our %conncfg=map{chomp;my @row=split/\=/;$row[0]=>$row[1] if $row[0]=~/^[^#]/} <CFG>;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
my @lvl;
my ($conf,$codetb,$codetblist,$maptype,$mapid,$str2num,$mappk,$mapobj,$mapeleid,$dateformat,$mapbizobj,$mappk);

my $dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
#DD��Ʒ��
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
	regexp_replace(T.TABLENAME,'^T_','DM_') TABLENAME,
	T.COLUMNNAME
	from dc_json_config t
	WHERE T.PRODUCTID = '".$productid."'
	AND T.COLUMNNAME IS NOT NULL
	AND T.TABLENAME IS NOT NULL
	AND T.TABLENAME NOT IN ('T_PA_PL_POLICY_ELEMENT','T_PA_POLICY_ELEMENT')
	AND NOT EXISTS(SELECT 1 FROM dc_invalid_column B
	WHERE T.TABLENAME=B.TABLENAME
	AND T.COLUMNNAME=B.COLUMNNAME)
	and not exists(select 1 from dc_invalid_table c
	where t.tablename=c.tablename)");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&writedblog($etlid,"","","E","dc_json_config���ò�����",&currenttime,&currenttime,"");
	exit 1;
}
#��װ��Ʒ������
while(my @resrow=$sth->fetchrow_array){
	push @lvl,$resrow[0];
	#�ڵ��Ӧ�ı���Ӧ���ֶΣ���Ӧ������
	$conf->{$resrow[0]}->{$resrow[8]}->{"table"}->{$resrow[10]}->{"col"}->{$resrow[11]}->{"attr"}=$resrow[3];
	#�������Զ�Ӧ�����
	$conf->{$resrow[0]}->{$resrow[8]}->{"codetable"}->{$resrow[3]}=$resrow[6]  if defined $resrow[6];
	#������ϵ��
	$conf->{$resrow[0]}->{$resrow[8]}->{'relation'}=$resrow[4] if defined $resrow[4];
	#��ǰ�ڵ�ĸ��ڵ�
	$conf->{$resrow[0]}->{$resrow[8]}->{'parent'}=$resrow[9] if defined $resrow[9];
	#��ʼ����α���
	@{$conf->{$resrow[0]}->{$resrow[8]}->{'pkval'}}=@pk if $resrow[0] == 1;
	#������ֶ�
	$conf->{$resrow[0]}->{$resrow[8]}->{'pkcol'}=$pkcol;
	$conf->{$resrow[0]}->{$resrow[8]}->{'fkcol'}=$fkcol;
	#���ݿⷶΧ�Ĺ�ϣȫ����Ϊ��д����Ҫӳ���DD���շ�
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($resrow[3])}=$resrow[3];
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($pkcol)}=$pkcol;
	$conf->{$resrow[0]}->{$resrow[8]}->{'attrmapping'}->{uc($fkcol)}=$fkcol;
	#�����Ӧ��ģ��
	$conf->{$resrow[0]}->{$resrow[8]}->{'modelname'}=$resrow[2];
	#������ϵ
	$relation->{$resrow[8]}->{$resrow[9]}=$resrow[4] if defined $resrow[4];
	#����嵥
	$codetblist->{$resrow[6]}="" if defined $resrow[6];
	#�ڵ��Ӧ�ĵ�ǰ��������ͣ���ʼ��������ͬһ�㼶��Ӧ�������
	$maptype->{$resrow[8]}=$resrow[1] if ! exists $maptype->{$resrow[8]};
}
$sth->finish;
$dbh->disconnect;

#codeӳ������
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
		 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
		exit 1;
	}
	$sth->execute();
	if(DBI->err){
		 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
		exit 1;
	}
	while(my @resrow=$sth->fetchrow_array){
		$codetb->{$resrow[0]}->{$resrow[2]}=$resrow[2];
	}
	$sth->finish();
	$dbh->disconnect();
}

#��������ת������
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select distinct a.modelname,b.field_name
 from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
 and a.productid = '".$productid."'
 and b.datatype in ('INTEGER', 'DOUBLE')");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$str2num->{$resrow[0]}->{$resrow[1]}="";
}
$sth->finish();
$dbh->disconnect();

#��������ת������
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("select distinct a.modelname,b.field_name
 from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
 and a.productid = '".$productid."'
 and b.datatype in ('DATE')");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$dateformat->{$resrow[0]}->{$resrow[1]}="";
}
$sth->finish();
$dbh->disconnect();

#objectcodeӳ������
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
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"objcode"}=$resrow[1];
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"elementcode"}=$resrow[3];
	$mapobj->{$resrow[0]}->{$resrow[7]}->{$resrow[6]}->{"type"}=$resrow[5];
}
$sth->finish();
$dbh->disconnect();

#businessobjidӳ������
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
$sth=$dbh->prepare("SELECT distinct a.parenttype||a.type, a.pk, a.objectcode
  from product_obj_tmp a, product_field_attr_tmp b
 where a.pk = b.pk
   and a.productid = '".$productid."'
   and b.field_name = 'BusinessObjectId'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute();
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
if($dbh->rows == 0){
	&writedblog($etlid,"",$productid,"E","product_obj/product_field_attr���ò�����",$startime,$startime,"");
	exit 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mapbizobj->{$resrow[0]}->{"id"}=$resrow[1];
	$mapbizobj->{$resrow[0]}->{"code"}=$resrow[2];
}
$sth->finish();
$dbh->disconnect();
#�㼶ȥ������
my %count;
@lvl=grep { ++$count{ $_ } < 2; } @lvl;
undef %count;
@lvl=sort @lvl;

#@pkӳ������
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
if(DBI->err){
	 &writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth=$dbh->prepare("select a.current_table,a.attr_name from dc_id_mapping_config a where a.status='1'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	return 1;
}
while(my @resrow=$sth->fetchrow_array){
	$mappk->{$resrow[0]}=$resrow[1];
}
$sth->finish();
$dbh->disconnect();

#���ݲ�Ʒ���㼶�Ӹ���Ҷ����̬����SQL�������ݿ������ݷ����ϣ
#��ѭ��Ʒ�㼶
foreach my $lv (@lvl){
	#��ѭ��Ʒ�ڵ�
	foreach my $path (keys%{$conf->{$lv}}){
		my $alltmpdata;
		my $fulltmpdata;
		#��ѭͬһ�ڵ��Ӧ�Ķ����
		foreach my $tablen (keys%{$conf->{$lv}->{$path}->{"table"}}){
			#����DDƴ�Ӷ�̬SQL�ֶ�
			my $sql="select /*+parallel($tablen,4)*/";
			foreach my $col (keys%{$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}}){
				$sql.=$col." as ".$conf->{$lv}->{$path}->{"table"}->{$tablen}->{"col"}->{$col}->{"attr"}.",";
			}
			#��������������
			if($lv ==1){
				$sql.=" ".$pkcol." from dc_inc.".$tablen." inner join dc_json_driver_$loopid on ".$conf->{$lv}->{$path}->{"pkcol"}."=driver_key where is_log='N'" ;	
			}else{
				$sql.=$pkcol.",".$fkcol." from dc_inc.".$tablen." inner join (select distinct driver_key from dc_json_driver_$loopid where lv=".($lv-1).") x on ".$conf->{$lv}->{$path}->{"fkcol"}."=x.driver_key where is_log='N'";
			}
			$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
			my $datasth=$dbh->prepare($sql);
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
				exit 1;
			}
			$datasth->execute();
			if(DBI->err){
				&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
				exit 1;
			}
			#��DC_PK�ֶ�Ϊ��ϣ����ȡ���ݿ��ϣ
			my $data=$datasth->fetchall_hashref($conf->{$lv}->{$path}->{"pkcol"});
			if($lv!=1){
				my $sthkey=$dbh->prepare("insert into dc_json_driver_$loopid (driver_key,lv) select dc_pk,".$lv." from dc_inc.$tablen a,dc_json_driver_$loopid b where a.dc_fk=b.driver_key and b.lv=$lv-1 and not exists(select 1 from dc_json_driver_$loopid c where dc_pk=c.driver_key and c.lv=$lv)");
				$sthkey->execute;
				$sthkey->finish;
			}
			$dbh->disconnect;
			#��ѭ������
			foreach my $datapk (keys%{$data}){
				my $tmpdata;
				#��ѭ�����е��ֶ�
				foreach my $attr(keys%{$data->{$datapk}}){
					$tmpdata->{"\@dcpk"}=$data->{$datapk}->{"DC_PK"};
					#�ǿ��жϣ���ֵ����
					if( defined $data->{$datapk}->{$attr}){
						#codemapping
						#�����ֶ�ӳ�䴦��
						if(exists($conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
							if(exists($codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}}->{$data->{$datapk}->{$attr}})){
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$codetb->{$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}->{$data->{$datapk}->{$attr}}};
							}else{
								delete $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								"";#&writecdmaplog($productid,$lv,$path,$attr,$datapk,$conf->{$lv}->{$path}->{"codetable"}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}},$data->{$datapk}->{$attr});
							}
						#�Ǵ����ֶδ���
						}else{
							$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=$data->{$datapk}->{$attr};
							#stringתnumber
							if(exists($str2num->{$conf->{$lv}->{$path}->{'modelname'}}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								if($tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~/^\./){
									$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}='0'.$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}};
								}
								"" if $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}} ==0;
							}
							#date��ʽ��
							if(exists($dateformat->{$conf->{$lv}->{$path}->{'modelname'}}->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}})){
								$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$attr}}=~s/\s/T/g;
							}
						}
					}
				}
				$tmpdata->{'@dctype'}=$path;
				#elementcodemapping
				if(exists($mapobj->{$conf->{$lv}->{$path}->{'modelname'}}) and exists($tmpdata->{'ProductElementCode'})){
					$tmpdata->{'@type'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"type"};
					$tmpdata->{'ProductElementCode'}=$mapobj->{$conf->{$lv}->{$path}->{'modelname'}}->{'ProductElementCode'}->{$tmpdata->{'ProductElementCode'}}->{"elementcode"};
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
				#�����������ö��������@pk
				$tmpdata->{'@pk'}=$tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}} if defined $tmpdata->{$conf->{$lv}->{$path}->{"attrmapping"}->{$mappk->{$tablen}}};
				next if ! defined $tmpdata->{'@pk'};
				"" if $tmpdata->{'@pk'}==0;
				push @{$conf->{$lv}->{$path}->{"pkval"}},$data->{$datapk}->{$conf->{$lv}->{$path}->{"pkcol"}} if $lv != 1;
				#�����������Ӧһ����������ݷ���ͬһ����
				foreach my $attrtmp(keys%{$tmpdata}){
					$fulltmpdata->{$datapk}->{$attrtmp}=$tmpdata->{$attrtmp};
				}
				undef $tmpdata;
			}
			
		}
		#���������ݷ���һ������
		foreach my $datapk(keys%{$fulltmpdata}){
			push @{$alltmpdata},$fulltmpdata->{$datapk};
		}
		undef $fulltmpdata;
		#��������ȥ��
		undef %count;
		@{$conf->{$lv}->{$path}->{"pkval"}}=grep { ++$count{ $_ } < 2; } @{$conf->{$lv}->{$path}->{"pkval"}};
		#����json����
		push @{$json->{$path}},@{$alltmpdata};
		undef $alltmpdata;
		#����ǰ�㼶��������Ϊ��һ�㼶�����
		foreach my $childtype (keys%{$conf->{$lv+1}}){
			if($conf->{$lv+1}->{$childtype}->{'parent'} eq $path and $lv+1 <= $lvl[-1]){
				@{$conf->{$lv+1}->{$childtype}->{'fkval'}}=@{$conf->{$lv}->{$path}->{"pkval"}};
			}
		}
	}
}

#���ݶ�����ϵ��װ���㼶��ϵ��JSON
#��ѭ��ϵ����
foreach my $type (keys%{$relation}){
	foreach my $parenttype (keys%{$relation->{$type}}){
		#��ѭ������
		for(my $parentidx=0;$parentidx < @{$json->{$parenttype}};$parentidx++){
			for(my $childidx=0;$childidx < @{$json->{$type}};$childidx++){
				#���ڵ��pk�����ӽڵ��fk����ô���Խ��Ӷ������
				if($json->{$parenttype}[$parentidx]->{"\@dcpk"} eq $json->{$type}[$childidx]->{$fkcol} and $parenttype eq $json->{$parenttype}[$parentidx]->{'@dctype'}){
					#productidӳ��
					$json->{$type}[$childidx]->{'ProductId'}=$productid if exists($json->{$type}[$childidx]->{'ProductId'});
					"" if $json->{$type}[$childidx]->{'ProductId'} == 0;
					#���ݲ�Ʒ���ڵ㼰����ӳ������ȷ����������
					$json->{$type}[$childidx]->{'@type'}=$maptype->{$type} if exists($maptype->{$json->{$type}[$childidx]->{'@dctype'}}) and ! exists($json->{$type}[$childidx]->{'@type'});
					next if ! defined $json->{$type}[$childidx]->{"\@type"};
					#ProductLobId api�������ֶΣ�ɾ��
					delete $json->{$type}[$childidx]->{"ProductLobId"} if exists $json->{$type}[$childidx]->{"ProductLobId"};
					push @{$json->{$parenttype}[$parentidx]->{$relation->{$type}->{$parenttype}}},$json->{$type}[$childidx];
				}
			}
		}
	}
}

#����ȫ·����ȡproductelementid
$dbh=DBI->connect('DBI:Oracle:'.$conncfg{"dbname"},$conncfg{"dbuser"},$conncfg{"dbpwd"});
#����ȫ·����Ӧ��businessobjectid/productelementid
$sth=$dbh->prepare("select a.parenttype || '/' || a.type, a.pk, a.elementid
  from product_obj_tmp a
 where a.productid = '".$productid."'");
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
$sth->execute;
if(DBI->err){
	&writedblog($etlid,"",$productid,DBI->err,DBI->errstr,$startime,&currenttime,"");
	exit 1;
}
my $mapeleid;
while(my @resrow=$sth->fetchrow_array){
	$mapeleid->{$resrow[0]}->{"objectid"}=$resrow[1];
	$mapeleid->{$resrow[0]}->{"elementid"}=$resrow[2] if defined $resrow[2];
}
$sth->finish;
$dbh->disconnect;

#set�����businessobjectid/productelementid
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
	#ɾ��DC�����ֶ�
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
#DC�����ֶ�
our @delcol=("DC_PK","DC_FK",'@dctype','@dcpk');
#��ѭ��Ʒ�ڵ�
foreach my $path(keys%{$json}){
	#POLICY���������ģ�ɾ����POLICY�ڵ�
	delete $json->{$path} and next if $path ne "/POLICY";
	#����ÿ�������ȫ·��������set����businessobjectid/productelementid����
	for(my $count=0;$count<@{$json->{$path}};$count++){
		$json->{$path}[$count]->{'@type'}='Policy-POLICY';
		setproductelement($json->{$path}[$count],"",$mapeleid);
	}
}

$filepath="/datafile/".$starttime."_".$productid.".txt";
open(FI,">>".$filepath);
for(my $count=0; $count<@{$json->{'/POLICY'}};$count++){
	my $jsstr=encode_json($json->{'/POLICY'}[$count]);
	print FI $jsstr."\n";
}
