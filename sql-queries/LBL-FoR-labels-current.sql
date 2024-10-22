select
	u.id as [user_id],
	u.[Computed Name Full],
	u.[Primary Group Descriptor],
	count(ul.Label) as [label_count],
	STRING_AGG(ul.[index], ';') as [label_indexes],
	STRING_AGG(ul.Label, ';') as [labels]
from
	[User] u
		left join [User Label] ul
			on u.id = ul.[User ID]
			and ul.[Scheme ID] = 1
where
	u.[Primary Group Descriptor] like ('%lbl%')
	and u.[Is Academic] = 1
	and u.[Is Current Staff] = 1
group by
	u.id,
	u.[Computed Name Full],
	u.[Primary Group Descriptor]
order by
	u.ID;
