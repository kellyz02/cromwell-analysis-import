const atlasConfig = {
  username: process.env.ATLAS_USERNAME,
  password: process.env.ATLAS_PASSWORD,
  baseUrl: process.env.ATLAS_BASE_URL,
  apiVer: process.env.ATLAS_API_VERSION,
  labGroup: process.env.ATLAS_API_LAB_GROUP,
  researcherEmail: process.env.ATLAS_RESEARCHER_EMAIL
};

module.exports = { atlasConfig };