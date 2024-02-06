 const axios = require('axios');  
const { ANALYSIS_MODULES_ENUM } = require('./enums.js'); 

const getAnalysisByRelated = async (xpId, token, atlasApiUrl, labGroup) => {
    const promises = ANALYSIS_MODULES_ENUM.map(element => axios.get(`${atlasApiUrl}/${labGroup}/ax/${element}?by_keywords=${xpId}&search_on=s_uid`,
        { headers: { Authorization: token } } // executes all promises simultaneously rather than one after the other 
    ));
    const res = await Promise.all(promises); // waits until all promises are resolved 
    return res.flatMap(element => element.data.data); // flatMap gets rid of internal arrays in res (res is array of arrays)
}

// searches by the ID, not by the Analysis UID
// ONLY FOR WGS ANALYSES, WILL ERROR OUT IF IT SEARCHES THROUGH THE OTHER MODULES WHERE THE ANALYSIS DOESN'T EXIST YET. 
const getWgsAnalysisById = async (id, token, atlasApiUrl, labGroup) => {
    return axios.get(`${atlasApiUrl}/${labGroup}/ax/wholegenomesequencing/${id}`, 
        { headers: { Authorization: token } }
    ); 
}

const getWgsAnalysisByUid = async (uid, token, atlasApiUrl, labGroup) => {
    return axios.get(`${atlasApiUrl}/${labGroup}/ax/wholegenomesequencing?by_keywords=AX${uid}&search_on=s_uid`, 
        { headers: { Authorization: token } }
    ); 
}


const updateAnalysis = async (analysisId, analysisType, fields, token, atlasApiUrl, labGroup) => {
    return axios.put(`${atlasApiUrl}/${labGroup}/ax/${analysisType}/${analysisId}`,
        fields,
        { headers: { Authorization: token } }
    ); 
}

module.exports = { getAnalysisByRelated, getWgsAnalysisById, getWgsAnalysisByUid, updateAnalysis }; 

