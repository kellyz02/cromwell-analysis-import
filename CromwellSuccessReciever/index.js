const { azureConfig } = require("../config/azure");
const { atlasConfig } = require('../config/atlas');

const { cromwellAccountKey, 
    workflowsContainer, 
    cromwellAccount, 
    atlasAccount, 
    atlasStagingContainer, 
    atlasAccountKey, 
    stagingFileDirectory } = azureConfig;
const { username, password, baseUrl, apiVer } = atlasConfig;

const { processDict, cleanDict } = require("../lib/helpers");  
const { cleanGeneral } = require("../lib/clean-workflows"); 
const { default: axios } = require("axios");

module.exports = async function (context, myBlob) {
    const { createUrl, readBlob, writeToBlob } = await import('@simalicrum/azure-helpers');
    const { auth, createAtlasApiUrl } = await import('atlas-helpers');

    try {

    // Create token for Atlas authentication 
    const atlasApiUrl = createAtlasApiUrl(baseUrl, apiVer); 
    const token = await auth(username, password, atlasApiUrl);  
    
    // Determine workflow type + inputs url
    const triggerUrl = createUrl("succeeded/" + context.bindingData.filename + "." + context.bindingData.workflowid + ".json", cromwellAccount, workflowsContainer); 
    const triggerJson = await readBlob(triggerUrl, cromwellAccountKey); 
    const triggerObject = JSON.parse(triggerJson); 

    const inputsUrl = triggerObject.WorkflowInputsUrl; 
    const inputsJson = await readBlob(inputsUrl, cromwellAccountKey); 
    const inputsObject = JSON.parse(inputsJson); 

    const optionsUrl = triggerObject.WorkflowOptionsUrl; 
    const wdlUrl = triggerObject.WorkflowUrl; 

    var firstKey = Object.keys(inputsObject)[0]; 
    const workflowType = firstKey.split(".")[0]; 

    // Check if workflowType is in dict. If so, get analysis based on workflowType: 
    if (!(workflowType in processDict)) {
        throw new Error("No automated clean-up available for this workflow!"); 
    }

    const studyKey = workflowType + ".study"; 
    let study = inputsObject[studyKey]; 
    if (!study || (study !== "pdx-samples" && study !== "non-cascadia" && study !== "cascadia")) {
        throw new Error("No study specified or study is not known of"); // may need to add any future studies to this guard
    }

    // Create or get Atlas analysis: 
    let axId; 
    axId = await processDict[workflowType](inputsUrl, inputsObject, token, atlasApiUrl, context); 
    // returns axID without "AX"
    context.log("Atlas analysis for this workflow: AX" + axId); 

    // Workflow-specific clean up: 
    let fileUrlArray = [];
    fileUrlArray = await cleanDict[workflowType](axId, context, fileUrlArray, inputsObject, token, atlasApiUrl, study); 

    // General clean up:
    fileUrlArray = await cleanGeneral(axId, context, fileUrlArray, inputsUrl, optionsUrl, wdlUrl); 
    const copiedFilesBody = fileUrlArray.toString(); 
    const copiedFilesUrl = createUrl(stagingFileDirectory + "/" + workflowType + "." + context.bindingData.workflowid + ".txt", atlasAccount, atlasStagingContainer); 
    const storeUrls = await writeToBlob(copiedFilesUrl, copiedFilesBody, atlasAccountKey); 
    context.log("Post workflow clean-up complete for " + context.bindingData.filename + "." + context.bindingData.workflowid + ".json"); 

    } catch(err) {
        context.log(err); 
        throw(err); 
    }
};