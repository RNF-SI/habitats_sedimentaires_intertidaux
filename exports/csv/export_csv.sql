DROP VIEW IF EXISTS gn_monitoring.v_export_habitats_sedimentaires_intertidaux_standard;

CREATE OR REPLACE VIEW gn_monitoring.v_export_habitats_sedimentaires_intertidaux_standard AS
WITH visites AS (
    SELECT
        v.id_base_visit,
        v.visit_date_min::text AS "date",
        v.id_base_site,
        b.base_site_name AS sous_station,
        st_y(b.geom) as latitude,
        st_x(b.geom) as longitude,
        sg.sites_group_name AS station,
        sg.data->>'site_opnl_name' as site_opnl_name,
        sg.data->>'id_station' as id_site,
        sg.id_sites_group,
        vc.data
    FROM gn_monitoring.t_base_visits v
    JOIN gn_monitoring.t_base_sites b ON v.id_base_site = b.id_base_site
    LEFT JOIN gn_monitoring.t_site_complements sc ON b.id_base_site = sc.id_base_site
    LEFT JOIN gn_monitoring.t_sites_groups sg ON sc.id_sites_group = sg.id_sites_group
    LEFT JOIN gn_monitoring.t_visit_complements vc ON v.id_base_visit = vc.id_base_visit
    WHERE v.id_dataset = 29 AND v.id_module = 16
),
observations AS (
    SELECT
        o.id_observation,
        o.id_base_visit,
        o.cd_nom,
        o.comments AS commentaire_obs,
        oc.data,
        o.uuid_observation,
        case when o.cd_nom != 0 then tx.lb_nom end as nom_taxon
    FROM gn_monitoring.t_observations o
    LEFT JOIN gn_monitoring.t_observation_complements oc ON o.id_observation = oc.id_observation
    left join taxonomie.taxref tx on o.cd_nom = tx.cd_nom 
),
fusion AS (
    SELECT
        v.id_base_visit,
        v.id_base_site,
        v."date",
        v.sous_station,
        v.longitude,
        v.latitude,
        v.station,
        v.site_opnl_name,
        v.id_sites_group,
        v.id_site,
        v.data->>'type_carotte'            AS type_carotte,
        v.data->>'taille_maille'           AS taille_maille_terrain,
        v.data->>'libelle_carotte'         AS num_carotte,
        v.data->>'matiere_organique'       AS pourcentage_mo,
        v.data->>'diametre_carrottier'     AS diam_carrotier,
        v.data->>'enfouissement_carrottier'AS enfouissement,
        o.cd_nom,
        o.commentaire_obs,
        o.nom_taxon,
        o.data->>'id_creuset'                          AS id_creuset,
        o.data->>'poids_tamis'                          AS pesee_tamis,
        o.data->>'nb_individus'                         AS nb_individu,
        o.data->>'determinateur'                        AS determinateur,
        o.data->>'masse_creuset'                        AS masse_creuset,
        o.data->>'taille_individu'                      AS taille,
        o.data->>'taille_maille_tamis'                  AS taille_maille_tamis,
        o.data->>'poids_tamis_sediment'                 AS pesee_tamis_sed,
        NULL::text                                      AS pesee_sed, -- champ manquant dans les compléments
        o.data->>'type_analyse_sediments'               AS type_analyse_sediments,
        o.data->>'type_analyse_macrofaune'              AS type_analyse_macrofaune,
        o.data->>'masse_creuset_sediment'               AS masse_creuset_sed,
        o.data->>'masse_creuset_sediment_libre_cendre'  AS masse_creuset_ldc,
        NULL::text                                      AS masse_mo,  -- champs non encore intégrés
        NULL::text                                      AS masse_sed,
        NULL::uuid                                      AS uuid_observation
    FROM visites v
    JOIN observations o ON v.id_base_visit = o.id_base_visit
)
SELECT
    f.site_opnl_name AS nom_site,
    f.station,
    f.id_site::bigint,
    f."date",
    f.sous_station,
    f.longitude,
    f.latitude,
    NULL::text AS commentaire_visit, -- non présent dans les tables, à compléter si besoin
    f.type_carotte,
    f.num_carotte,
    f.diam_carrotier::bigint,
    f.enfouissement::bigint,
    f.taille_maille_terrain,
    f.determinateur,
    f.commentaire_obs,
    COALESCE(f.type_analyse_macrofaune, f.type_analyse_sediments) AS type_analyse,
    f.nom_taxon,
    f.cd_nom::bigint,
    f.nb_individu,
    f.taille,
    f.taille_maille_tamis,
    f.pesee_sed,
    f.pesee_tamis_sed,
    f.pesee_tamis,
    NULL::text AS poids_eau,
    f.id_creuset,
    f.masse_creuset,
    f.masse_creuset_sed,
    f.masse_creuset_ldc,
    f.masse_mo,
    f.masse_sed,
    f.pourcentage_mo
FROM fusion f;