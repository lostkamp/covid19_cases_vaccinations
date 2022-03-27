drop table if exists staging_cases;

create table staging_cases (
  IdBundesland int,
  Bundesland text,
  Landkreis text,
  Altersgruppe text,
  Geschlecht text,
  AnzahlFall int,
  AnzahlTodesfall int,
  ObjectId int,
  Meldedatum text,
  IdLandkreis text,
  Datenstand text,
  NeuerFall int,
  NeuerTodesfall int,
  Refdatum text,
  NeuGenesen int,
  AnzahlGenesen int,
  IstErkrankungsbeginn int,
  Altersgruppe2 text,
  MeldedatumISO text,
  DatenstandISO text,
  RefdatumISO text
);
