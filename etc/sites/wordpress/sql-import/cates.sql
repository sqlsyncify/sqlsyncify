-- key=wtt.term_id
SELECT wtt.term_id AS cat_id, wt.name AS cat_name
     , IFNULL(CONCAT('[', getCategoryParentListObj (wtt.term_id),']'),'[]')  AS categories
FROM wp_term_taxonomy AS wtt
          join wp_terms wt on wtt.term_id = wt.term_id
WHERE wtt.taxonomy in ('category','product_cat')

