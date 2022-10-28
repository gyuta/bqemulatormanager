SELECT
  code,
  service_name
FROM (
  SELECT
    user_use_list.code,
    service.name AS service_name
  FROM
    `dataset1.user_use_list` user_use_list
  INNER JOIN
    `dataset1.service` service
  ON
    user_use_list.service_id = service.id )
UNION DISTINCT (
  SELECT
    users.code,
    service_name
  FROM (
    SELECT
      package_use_list.ID,
      service.name AS service_name
    FROM
      `dataset1.package_use_list` package_use_list
    INNER JOIN
      `dataset1.service` service
    ON
      package_use_list.SERVICE_ID = service.id ) lst
  INNER JOIN
    `dataset1.users` users
  ON
    users.ID = CAST(lst.ID AS STRING) )
