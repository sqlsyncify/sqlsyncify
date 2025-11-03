SELECT wtt.term_id AS cat_id, wtt.parent AS parent_id, wt.name AS cat_name
     , IFNULL(CONCAT('[', getCategoryParentListObj (wtt.term_id),']'),'[]')  AS categories
FROM wp_term_taxonomy AS wtt
         left join wp_terms wt on wtt.term_id = wt.term_id
WHERE 1=1
 and wtt.taxonomy in ('category','product_cat')
  and wt.term_id is not null
