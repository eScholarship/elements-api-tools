SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
BEGIN TRANSACTION;

WITH full_counts AS (
	SELECT
    	u.ID,
		u.Email,
    	u.[Computed Name Full] as [Name],
    	u.[Primary Group Descriptor],
    	pl.Label,
    	COUNT(pl.[Publication ID]) as [Pub Count],
    	[Label Rank] = RANK() OVER (
			PARTITION BY u.ID
			ORDER BY COUNT(pl.[Publication ID]) desc)
	FROM
		[User] u
    	join [Publication User Relationship] pur
			on u.id = pur.[User ID]
		join [Publication Label] pl
        	on pur.[Publication ID] = pl.[Publication ID]
        	and pl.[Scheme ID] = 1
	WHERE
    	u.[Primary Group Descriptor] = 'lbl-user'
     	and pl.Label like '[0-9][0-9][0-9][0-9]%'
	GROUP BY
    	u.ID,
    	u.Email,
    	u.[Computed Name Full],
    	u.[Primary Group Descriptor],
    	pl.Label
)
SELECT
	*
FROM
	full_counts
WHERE
	[Label Rank] <= 5
ORDER BY
	ID,
	[Label Rank] asc,
	[Label];

COMMIT TRANSACTION;