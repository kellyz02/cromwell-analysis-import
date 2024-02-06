const { azureConfig } = require("../config/azure");
const { atlasConfig } = require('../config/atlas');
const { analysisFields } = require("./analysis-fields");  
const { getAnalysisByRelated, getWgsAnalysisById, updateAnalysis } = require("./atlas-helpers"); 
const { default: axios } = require("axios");
const { format } = require("date-fns"); 
const path = require("path"); 

const { cromwellAccountKey } = azureConfig;
const { labGroup, researcherEmail } = atlasConfig;
const wgs = "wholegenomesequencing"; // ok as a variable? 

// Get "last modified" date of inputs.json file -> requires user to upload new inputs.json file every run
// date must be formatted YYYY-MM-DD for Atlas
// uses date-fns library to format date 
const getInputsDate = async (inputsUrl) => { 
    const { createBlobClient } = await import("@simalicrum/azure-helpers"); 
    
    const inputsBlobClient = createBlobClient(inputsUrl, cromwellAccountKey); 
    const properties = await inputsBlobClient.getProperties(); 
    let uploadDate = properties.lastModified; 
    uploadDate = format(uploadDate, "yyyy'-'MM'-'dd"); 
    return uploadDate; 
}

// Checks for pre-existing analysis objects that match related input id (AX or XP) and appropriate analysis type 
const checkAnalyses = async (relatedId, analysisType, analysisSubType, analysisReference, token, atlasApiUrl, context) => { 
    const analyses = await getAnalysisByRelated(relatedId, token, atlasApiUrl, labGroup); 
    
    let axString = ""
    for (let a of analyses) {
        axString += "AX" + a.fk_ax_uid + " "; 
    }
    context.log("Check returned possible matching analyses: " + axString); 

    for (let i = 0; i < analyses.length; i++) {
        if (analyses[i].s_analysis_type == analysisType && analyses[i].s_analysis_subtype == analysisSubType && analyses[i].s_reference_file == analysisReference) { 
            context.log("Check found matching pre-existing analysis: AX" + analyses[i].fk_ax_uid); 
            return analyses[i].fk_ax_uid;  
        }
    }
    context.log("Check didn't find any matching pre-existing analysis. Creating a new analysis!")
    return ""; // If there's a pre-existing analysis, return AXID. Otherwise, return empty string 
}

// Create CleanPDX Atlas analysis 
 const processCleanPDX = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis } = await import('atlas-helpers'); 
    // get input data
    const uploadDate = await getInputsDate(inputsUrl); 
    const externalId = inputsObject["CleanPDX.sample_ID"]; 
    const experiment = await getExperimentbyExternalId(externalId, wgs, token, atlasApiUrl, labGroup);
    if (!experiment || experiment.data === [] || !(experiment.data.data[0].i_xp_uid)) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!") 
    }
    const xpUid = experiment.data.data[0].i_xp_uid; // returns XPID w/o XP

    let analysisAx = await checkAnalyses("XP" + xpUid, 'Mouse Read Filtering', '', "hg38", token, atlasApiUrl, context); 

    if (analysisAx === "") {
        // create new analysis if null
        const fields = analysisFields["CleanPDX"](uploadDate, researcherEmail, [xpUid], "hg38"); 

        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); 
        const analysisId = newAnalysis.data.data; 

        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx); 
    }
    return analysisAx; 
 }

// Create CustomPreProcessing analysis 
 const processCustomPrePro = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const externalId = inputsObject["PreProcessing.sample_and_unmapped_bams"]["base_file_name"]; 
    const experiment = await getExperimentbyExternalId(externalId, wgs, token, atlasApiUrl, labGroup); 
    if (!experiment || experiment.data === [] || !(experiment.data.data[0].i_xp_uid)) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!") 
    }
    const xpUid = experiment.data.data[0].i_xp_uid;
    const refPath = inputsObject["PreProcessing.references"]["reference_fasta"]["ref_fasta"]; 
    let ref = path.basename(refPath); 

    // if workflows are run with different references, add them to here. 
    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    let analysisAx = await checkAnalyses("XP" + xpUid, 'Pre-processing', 'GATK Pre-processing Pipeline', ref, token, atlasApiUrl, context); 

    if (analysisAx === "") {
        let analyses = await getAnalysisByRelated("XP" + xpUid, token, atlasApiUrl, labGroup); 
        let prevAnalysisId; 
        if (analyses.find(element => element.s_analysis_type === "Mouse Read Filtering").fk_ax_uid) {
            prevAnalysisId = analyses.find(element => element.s_analysis_type === "Mouse Read Filtering").fk_ax_uid; 
        } else {
            throw new Error("Related CleanPDX analysis cannot be found by XPID. Aborting cleanup!"); 
        }

        const fields = analysisFields["PreProcessing"](uploadDate, researcherEmail, [prevAnalysisId], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); 
        const analysisId = newAnalysis.data.data; 

        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx); 
    }
    return analysisAx; 
 }

// Create UbamPrePro analysis 
 const processUbamPrePro = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const externalId = inputsObject["UbamPrePro.sample_info"]["base_file_name"];
    const experiment = await getExperimentbyExternalId(externalId, wgs, token, atlasApiUrl, labGroup); 
    if (!experiment || experiment.data === [] || !(experiment.data.data[0].i_xp_uid)) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!") 
    }
    const xpUid = experiment.data.data[0].i_xp_uid; 
    const refPath = inputsObject["UbamPrePro.references"]["reference_fasta"]["ref_fasta"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }
    
    let analysisAx = await checkAnalyses("XP" + xpUid, 'Pre-processing', 'GATK Pre-processing Pipeline', ref, token, atlasApiUrl, context); 

    if (analysisAx === "") {
        const fields = analysisFields["UbamPrePro"](uploadDate, researcherEmail, [xpUid], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup);  
        const analysisId = newAnalysis.data.data; 

        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx); 
    }

    return analysisAx; 
 }

// Create UbamGermPrePro analyses
 const processUbamGermPrePro = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {  
    const { getExperimentbyExternalId, addAnalysis, getAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const externalId = inputsObject["UbamGermlinePrePro.sample_info"]["base_file_name"]; 
    const experiment = await getExperimentbyExternalId(externalId, wgs, token, atlasApiUrl, labGroup);
    if (!experiment || experiment.data === [] || !(experiment.data.data[0].i_xp_uid)) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!") 
    }
    const xpUid = experiment.data.data[0].i_xp_uid; 

    const refPath = inputsObject["UbamGermlinePrePro.references"]["reference_fasta"]["ref_fasta"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    let analysisAx = await checkAnalyses("XP" + xpUid, 'Pre-processing', 'GATK Pre-processing Pipeline', ref, token, atlasApiUrl, context);
    let htcAx; 

    if (analysisAx === "") {
        const fields = analysisFields["UbamGermlinePrePro"](uploadDate, researcherEmail, [xpUid], ref);  
    
        // PrePro Analysis: 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); 
        const analysisId = newAnalysis.data.data; 
        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx); 

        // HTC Analysis:
        htcAx = await processHtc(analysisAx, uploadDate, token, atlasApiUrl, ref, context); 
    } else {
        let htcAnalyses = await getAnalysis(analysisAx, token, atlasApiUrl, labGroup); 
        if (htcAnalyses.find(element => element.s_analysis_type === "Germline Short Variant Calling") === undefined) {
            htcAx = await processHtc(analysisAx, uploadDate, token, atlasApiUrl, ref, context); 
        } else {
            htcAx = htcAnalyses.find(element => element.s_analysis_type === "Germline Short Variant Calling").fk_ax_uid; 
        }
    }

    return [analysisAx, htcAx]; 
 }

// Create HaplotypeCaller analysis 
 const processHtc = async (relatedAx, uploadDate, token, atlasApiUrl, ref, context) => {
    const { addAnalysis } = await import('atlas-helpers'); 

    let analysisAx = await checkAnalyses("AX" + relatedAx, 'Germline Short Variant Calling', 'GATK HaplotypeCaller', ref, token, atlasApiUrl, context); 

    if (analysisAx === "") {
        const fields = analysisFields["Htc"](uploadDate, researcherEmail, [relatedAx], ref);
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); 
        const analysisId = newAnalysis.data.data; 

        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx); 
    }

    return analysisAx; 
 }

// Create Mutect2-Cbio analyses 
 const processMutect2Cbio = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis, getAnalysis, deleteAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const tumorExternalId = inputsObject["Mutect2.tumor_id"]; 
    const normalExternalId = inputsObject["Mutect2.normal_id"]; 

    const tumorExperiment = await getExperimentbyExternalId(tumorExternalId, wgs, token, atlasApiUrl, labGroup); 
    const normalExperiment = await getExperimentbyExternalId(normalExternalId, wgs, token, atlasApiUrl, labGroup); 

    if (!tumorExperiment.data.data[0].i_xp_uid || !normalExperiment.data.data[0].i_xp_uid) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!"); 
    }

    const tumourXpUid = "XP" + tumorExperiment.data.data[0].i_xp_uid;
    const normalXpUid = "XP" + normalExperiment.data.data[0].i_xp_uid;

    const refPath = inputsObject["Mutect2.ref_fasta"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    // get tumor UbamPrePro analysis
    let tumorAnalyses = await getAnalysisByRelated(tumourXpUid, token, atlasApiUrl, labGroup); 
    let tumorRelatedId; 
    if (tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        tumorRelatedId = tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid;
    } else {
        throw new Error("Related tumor pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    // get normal UbamGermPrePro analysis
    let normalAnalyses = await getAnalysisByRelated(normalXpUid, token, atlasApiUrl, labGroup);
    let normalRelatedId; 
    if (normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        normalRelatedId = normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid; 
    } else {
        throw new Error("Related germline pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    let analysisAx = await checkAnalyses("AX" + tumorRelatedId, 'Somatic Short Variant Calling', 'GATK Mutect2 Tumour-Normal Paired', ref, token, atlasApiUrl, context) 
    let cbioAx; 

    if (analysisAx === "") {
        const fields = analysisFields["Mutect2"](uploadDate, researcherEmail, [tumorRelatedId, normalRelatedId], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); // automatically only makes 1 analysis 
        const analysisId = newAnalysis.data.data; 
        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx);  

        cbioAx = await processCbio(analysisAx, uploadDate, token, atlasApiUrl, ref, context); 
    } else {
        let cbioAnalyses = await getAnalysisByRelated("AX" + analysisAx, token, atlasApiUrl, labGroup); 
        if (cbioAnalyses.find(element => element.s_analysis_type === "MAF Annotation for cBioPortal") === undefined) {
            cbioAx = await processCbio(analysisAx, uploadDate, token, atlasApiUrl, ref, context);
        } else {
            cbioAx = cbioAnalyses.find(element => element.s_analysis_type === "MAF Annotation for cBioPortal").fk_ax_uid; 
        }
    }
    return [analysisAx, cbioAx]; 
 }

// Create Cbio analysis 
 const processCbio = async (relatedAx, uploadDate, token, atlasApiUrl, ref, context) => {
    const { addAnalysis } = await import('atlas-helpers'); 
    // not actually necessary, not possible for it to exist if we just created relatedAx
    let analysisAx = await checkAnalyses(relatedAx, 'MAF Annotation for cBioPortal', '', ref, token, atlasApiUrl, context);

    if (analysisAx === "") {
        const fields = analysisFields["Cbio"](uploadDate, researcherEmail, [relatedAx], ref);
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); 
        const analysisId = newAnalysis.data.data; 

        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid;
        context.log("Created new analysis: AX" + analysisAx); 
    }

    return analysisAx; 
 }


// STILL IN TESTING. 
// Create ReadCounterIchor analyses 
// the same parameters are passed in for all workflows. 
 const processReadCounterIchor = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis, getAnalysis, deleteAnalysis } = await import('atlas-helpers'); 

    // get approx. workflow execution date
    const uploadDate = await getInputsDate(inputsUrl); 
    
    // access the inputs.json object for the library/external IDs of all samples used
    const normalExternalId = inputsObject["ReadCounterIchor.rc_normal_inputs"]["normal_id"]; 
    const tumorInputsArray = inputsObject["ReadCounterIchor.rc_tumor_inputs"]; 
    
    let tumorExternalIdsArray; 
    for (let tumorInput of tumorInputsArray) {
        tumorExternalIdsArray.push(tumorInput["left"]); 
    }

    let axIdArray; // array to store the AX IDs of analysis objects for the workflow

    for (let tumorExternalId of tumorExternalIdsArray) {
        // find corresponding Atlas experiment objects using external IDs
        const tumorExperiment = await getExperimentbyExternalId(tumorExternalId, wgs, token, atlasApiUrl, labGroup); 
        const normalExperiment = await getExperimentbyExternalId(normalExternalId, wgs, token, atlasApiUrl, labGroup); 

        if (!tumorExperiment.data.data[0].i_xp_uid || !normalExperiment.data.data[0].i_xp_uid) {
            throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!"); 
        }

        // Get the XP ID from the experiment object
        const tumourXpUid = "XP" + tumorExperiment.data.data[0].i_xp_uid;
        const normalXpUid = "XP" + normalExperiment.data.data[0].i_xp_uid;

        // check the reference used for the workflow
        const refPath = inputsObject["ReadCounterIchor.ichor_references"]["gc_wig"]; 
        let ref = path.basename(refPath); 

        if (ref === "gc_hg38_1000kb.wig") {
            ref = "hg38"; 
        } else {
            throw new Error('Reference file not known. Aborting cleanup!'); 
        }

        // get the related tumour + germline analyses (this code is identical to Mutect2)
        let tumorAnalyses = await getAnalysisByRelated(tumourXpUid, token, atlasApiUrl, labGroup); 
        let tumorRelatedId; 
        if (tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
            tumorRelatedId = tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid;
        } else {
            throw new Error("Related tumor pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
        }

        // get normal UbamGermPrePro (pre-processing) analysis 
        let normalAnalyses = await getAnalysisByRelated(normalXpUid, token, atlasApiUrl, labGroup);
        let normalRelatedId; 
        if (normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
            normalRelatedId = normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid; 
        } else {
            throw new Error("Related germline pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
        }

        let analysisAx = await checkAnalyses("AX" + tumorRelatedId, 'Copy Number Analysis', '', ref, token, atlasApiUrl, context)
             
        if (analysisAx === "") {
            // if an empty string is returned, create a new analysis object. 
            const fields = analysisFields["ReadCounterIchor"](uploadDate, researcherEmail, [tumorRelatedId, normalRelatedId], ref); 
            const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); // automatically only makes 1 analysis overall, not 1 for each related input.
            const analysisId = newAnalysis.data.data; 
            const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
            analysisAx = analysisDetails.data.data.fk_ax_uid; 
            context.log("Created new analysis: AX" + analysisAx);  
        }

        axIdArray.push(analysisAx); 
    }

    // return all AXIDs to main function after processing all pairs. the AX IDs are ordered in the same order as the sample IDs appear in the inputs.json
    return axIdArray; 
 }

// Create Sequenza analysis
 const processSequenzaWorkflow = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis, getAnalysis, deleteAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const tumorExternalId = inputsObject["SequenzaWorkflow.tumorName"]; 
    const normalExternalId = inputsObject["SequenzaWorkflow.normalName"]; 

    const tumorExperiment = await getExperimentbyExternalId(tumorExternalId, wgs, token, atlasApiUrl, labGroup); 
    const normalExperiment = await getExperimentbyExternalId(normalExternalId, wgs, token, atlasApiUrl, labGroup); 

    if (!tumorExperiment.data.data[0].i_xp_uid || !normalExperiment.data.data[0].i_xp_uid) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!"); 
    }

    const tumourXpUid = "XP" + tumorExperiment.data.data[0].i_xp_uid;
    const normalXpUid = "XP" + normalExperiment.data.data[0].i_xp_uid;

    const refPath = inputsObject["SequenzaWorkflow.ReferenceFastaGz"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta.gz") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    // get tumor UbamPrePro analysis
    let tumorAnalyses = await getAnalysisByRelated(tumourXpUid, token, atlasApiUrl, labGroup); 
    let tumorRelatedId; 
    if (tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        tumorRelatedId = tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid;
    } else {
        throw new Error("Related tumor pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    // get normal UbamGermPrePro analysis
    let normalAnalyses = await getAnalysisByRelated(normalXpUid, token, atlasApiUrl, labGroup);
    let normalRelatedId; 
    if (normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        normalRelatedId = normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid; 
    } else {
        throw new Error("Related germline pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    let analysisAx = await checkAnalyses("AX" + tumorRelatedId, 'Copy Number Analysis', '', ref, token, atlasApiUrl, context)
             
    if (analysisAx === "") {
        // if an empty string is returned, create a new analysis object. 
        const fields = analysisFields["SequenzaWorkflow"](uploadDate, researcherEmail, [tumorRelatedId, normalRelatedId], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); // automatically only makes 1 analysis overall, not 1 for each related input.
        const analysisId = newAnalysis.data.data; 
        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx);  
    }

    return analysisAx; 

 }

// Create WASP analysis
 const processWaspMapping = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis, getAnalysis, deleteAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const tumorExternalId = inputsObject["WaspMapping.sample_name"]; 
    const normalExternalId = inputsObject["WaspMapping.normal_name"]; 

    const tumorExperiment = await getExperimentbyExternalId(tumorExternalId, wgs, token, atlasApiUrl, labGroup); 
    const normalExperiment = await getExperimentbyExternalId(normalExternalId, wgs, token, atlasApiUrl, labGroup); 

    if (!tumorExperiment.data.data[0].i_xp_uid || !normalExperiment.data.data[0].i_xp_uid) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!"); 
    }

    const tumourXpUid = "XP" + tumorExperiment.data.data[0].i_xp_uid;
    const normalXpUid = "XP" + normalExperiment.data.data[0].i_xp_uid;

    const refPath = inputsObject["WaspMapping.references"]["ref_fasta"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    // get tumor UbamPrePro analysis
    let tumorAnalyses = await getAnalysisByRelated(tumourXpUid, token, atlasApiUrl, labGroup); 
    let tumorRelatedId; 
    if (tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        tumorRelatedId = tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid;
    } else {
        throw new Error("Related tumor pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    // get normal UbamGermPrePro analysis
    let normalAnalyses = await getAnalysisByRelated(normalXpUid, token, atlasApiUrl, labGroup);
    let normalRelatedId; 
    if (normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        normalRelatedId = normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid; 
    } else {
        throw new Error("Related germline pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    let analysisAx = await checkAnalyses("AX" + tumorRelatedId, 'Copy Number Analysis', '', ref, token, atlasApiUrl, context)
             
    if (analysisAx === "") {
        // if an empty string is returned, create a new analysis object. 
        const fields = analysisFields["WaspMapping"](uploadDate, researcherEmail, [tumorRelatedId, normalRelatedId], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); // automatically only makes 1 analysis overall, not 1 for each related input.
        const analysisId = newAnalysis.data.data; 
        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx);  
    }

    return analysisAx; 

 }

 // Create SV Caller analysis
 const processPairedSVCaller = async (inputsUrl, inputsObject, token, atlasApiUrl, context) => {
    const { getExperimentbyExternalId, addAnalysis, getAnalysis, deleteAnalysis } = await import('atlas-helpers'); 

    const uploadDate = await getInputsDate(inputsUrl); 
    const tumorExternalId = inputsObject["PairedSVCaller.sample_name"]; 
    const normalExternalId = inputsObject["PairedSVCaller.normal_name"]; 

    const tumorExperiment = await getExperimentbyExternalId(tumorExternalId, wgs, token, atlasApiUrl, labGroup); 
    const normalExperiment = await getExperimentbyExternalId(normalExternalId, wgs, token, atlasApiUrl, labGroup); 

    if (!tumorExperiment.data.data[0].i_xp_uid || !normalExperiment.data.data[0].i_xp_uid) {
        throw new Error("Related experiment cannot be found by external ID; please add external ID on Atlas. Aborting cleanup!"); 
    }

    const tumourXpUid = "XP" + tumorExperiment.data.data[0].i_xp_uid;
    const normalXpUid = "XP" + normalExperiment.data.data[0].i_xp_uid;

    const refPath = inputsObject["PairedSVCaller.references"]["ref_fasta"]; 
    let ref = path.basename(refPath); 

    if (ref === "Homo_sapiens_assembly38.fasta") {
        ref = "hg38"; 
    } else {
        throw new Error('Reference file not known. Aborting cleanup!'); 
    }

    // get tumor UbamPrePro analysis
    let tumorAnalyses = await getAnalysisByRelated(tumourXpUid, token, atlasApiUrl, labGroup); 
    let tumorRelatedId; 
    if (tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        tumorRelatedId = tumorAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid;
    } else {
        throw new Error("Related tumor pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    // get normal UbamGermPrePro analysis
    let normalAnalyses = await getAnalysisByRelated(normalXpUid, token, atlasApiUrl, labGroup);
    let normalRelatedId; 
    if (normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid) {
        normalRelatedId = normalAnalyses.find(element => element.s_analysis_type === "Pre-processing").fk_ax_uid; 
    } else {
        throw new Error("Related germline pre-processing analysis cannot be found by XPID. Aborting cleanup!"); 
    }

    let analysisAx = await checkAnalyses("AX" + tumorRelatedId, 'Copy Number Analysis', '', ref, token, atlasApiUrl, context)
             
    if (analysisAx === "") {
        // if an empty string is returned, create a new analysis object. 
        const fields = analysisFields["PairedSVCaller"](uploadDate, researcherEmail, [tumorRelatedId, normalRelatedId], ref); 
        const newAnalysis = await addAnalysis(fields, wgs, token, atlasApiUrl, labGroup); // automatically only makes 1 analysis overall, not 1 for each related input.
        const analysisId = newAnalysis.data.data; 
        const analysisDetails = await getWgsAnalysisById(analysisId, token, atlasApiUrl, labGroup); 
        analysisAx = analysisDetails.data.data.fk_ax_uid; 
        context.log("Created new analysis: AX" + analysisAx);  
    }

    return analysisAx; 

 }

module.exports = { processCleanPDX, processUbamPrePro, processCustomPrePro, processUbamGermPrePro, processMutect2Cbio, processCbio, processHtc, processReadCounterIchor, processSequenzaWorkflow, processWaspMapping, processPairedSVCaller };





