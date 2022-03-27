drop table if exists staging_vaccinations;

create table staging_vaccinations (
    Impfdatum text,
    LandkreisId_Impfort text,
    Altergruppe text,
    Impfschutz int,
    Anzahl int
);
