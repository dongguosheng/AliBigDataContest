sql('drop table if exists t_pair_proba_norm;')
DataProc.normalize('t_pair_proba', 't_pair_proba_norm', selectedColNames=['proba0', 'proba1', 'proba2', 'proba3', 'proba4', 'proba5', 'proba6', 'proba7', 'proba8', 'proba9', 'proba10', 'proba11', 'proba12', 'proba13', 'proba14', 'proba15', 'proba16', 'proba17', 'proba18', 'proba19'])
sql('''
drop table if exists t_proba_gbrt;
create table t_proba_gbrt as
select user_id, brand_id, (p0+p1+p2+p3+p4+p5+p6+p7+p8+p9+p10+p11+p12+p13+p14+p15+p16+p17+p18+p19) / (num0+num1+num2+num3+num4+num5+num6+num7+num8+num9+num10+num11+num12+num13+num14+num15+num16+num17+num18+num19) as proba
from (
select user_id, brand_id, 
case 
when proba0 > 0.5 then 1
else 0
end
as num0,
case 
when proba1 > 0.5 then 1
else 0
end
as num1,
case 
when proba2 > 0.5 then 1
else 0
end
as num2,
case 
when proba3 > 0.5 then 1
else 0
end
as num3,
case 
when proba4 > 0.5 then 1
else 0
end
as num4,
case 
when proba5 > 0.5 then 1
else 0
end
as num5,
case 
when proba6 > 0.5 then 1
else 0
end
as num6,
case 
when proba7 > 0.5 then 1
else 0
end
as num7,
case 
when proba8 > 0.5 then 1
else 0
end
as num8,
case 
when proba9 > 0.5 then 1
else 0
end
as num9,
case 
when proba10 > 0.5 then 1
else 0
end
as num10,
case 
when proba11 > 0.5 then 1
else 0
end
as num11,
case 
when proba12 > 0.5 then 1
else 0
end
as num12,
case 
when proba13 > 0.5 then 1
else 0
end
as num13,
case 
when proba14 > 0.5 then 1
else 0
end
as num14,
case 
when proba15 > 0.5 then 1
else 0
end
as num15,
case 
when proba16 > 0.5 then 1
else 0
end
as num16,
case 
when proba17 > 0.5 then 1
else 0
end
as num17,
case 
when proba18 > 0.5 then 1
else 0
end
as num18,
case 
when proba19 > 0.5 then 1
else 0
end
as num19,
case 
when proba0 > 0.5 then proba0
else 0
end
as p0,
case 
when proba1 > 0.5 then proba1
else 0
end
as p1,
case 
when proba2 > 0.5 then proba2
else 0
end
as p2,
case 
when proba3 > 0.5 then proba3
else 0
end
as p3,
case 
when proba4 > 0.5 then proba4
else 0
end
as p4,
case 
when proba5 > 0.5 then proba5
else 0
end
as p5,
case 
when proba6 > 0.5 then proba6
else 0
end
as p6,
case 
when proba7 > 0.5 then proba7
else 0
end
as p7,
case 
when proba8 > 0.5 then proba8
else 0
end
as p8,
case 
when proba9 > 0.5 then proba9
else 0
end
as p9,
case 
when proba10 > 0.5 then proba10
else 0
end
as p10,
case 
when proba11 > 0.5 then proba11
else 0
end
as p11,
case 
when proba12 > 0.5 then proba12
else 0
end
as p12,
case 
when proba13 > 0.5 then proba13
else 0
end
as p13,
case 
when proba14 > 0.5 then proba14
else 0
end
as p14,
case 
when proba15 > 0.5 then proba15
else 0
end
as p15,
case 
when proba16 > 0.5 then proba16
else 0
end
as p16,
case 
when proba17 > 0.5 then proba17
else 0
end
as p17,
case 
when proba18 > 0.5 then proba18
else 0
end
as p18,
case 
when proba19 > 0.5 then proba19
else 0
end
as p19 
from t_pair_proba_norm
)a
where num0+num1+num2+num3+num4+num5+num6+num7+num8+num9+num10+num11+num12+num13+num14+num15+num16+num17+num18+num19 > 0;
''')
