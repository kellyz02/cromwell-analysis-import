const { processCleanPDX, processCustomPrePro, processUbamPrePro, processUbamGermPrePro, processMutect2Cbio, processCbio, processHtc, processReadCounterIchor, processSequenzaWorkflow, processWaspMapping, processPairedSVCaller } = require("./process-workflows");
const { analysisCleanPDX, analysisCustomPrePro, analysisUbamPrePro, analysisUbamGermPrePro, analysisMutect2, analysisHtc, analysisCbio, analysisReadCounterIchor, analysisSequenzaWorkflow, analysisWaspMapping, analysisPairedSVCaller  } = require("./analysis-fields"); 
const { cleanCleanPDX, cleanCustomPrePro, cleanUbamPrePro, cleanUbamGermPrePro, cleanMutect2Cbio, cleanCbio, cleanHtc, cleanReadCounterIchor, cleanSequenzaWorkflow, cleanWaspMapping, cleanPairedSVCaller } = require("./clean-workflows"); 

const analysisDict = { // keys should match string elements extracted from inputs.json file 
    "CleanPDX": analysisCleanPDX, 
    "PreProcessing": analysisCustomPrePro,
    "UbamPrePro": analysisUbamPrePro, 
    "UbamGermlinePrePro": analysisUbamGermPrePro, 
    "Mutect2": analysisMutect2, 
    "Cbio": analysisCbio, 
    "Htc": analysisHtc, 
    "ReadCounterIchor": analysisReadCounterIchor ,
    "SequenzaWorkflow": analysisSequenzaWorkflow,
    "WaspMapping": analysisWaspMapping,
    "PairedSVCaller": analysisPairedSVCaller
} 

const processDict = {
    "CleanPDX": processCleanPDX, 
    "PreProcessing": processCustomPrePro,
    "UbamPrePro": processUbamPrePro, 
    "UbamGermlinePrePro": processUbamGermPrePro, 
    "Mutect2": processMutect2Cbio, 
    "Cbio": processCbio, 
    "Htc": processHtc, 
    "ReadCounterIchor": processReadCounterIchor,
    "SequenzaWorkflow": processSequenzaWorkflow,
    "WaspMapping": processWaspMapping,
    "PairedSVCaller": processPairedSVCaller
}

const cleanDict = {
    "CleanPDX": cleanCleanPDX, 
    "PreProcessing": cleanCustomPrePro,
    "UbamPrePro": cleanUbamPrePro, 
    "UbamGermlinePrePro": cleanUbamGermPrePro, 
    "Mutect2": cleanMutect2Cbio, 
    "Cbio": cleanCbio, 
    "Htc": cleanHtc,
    "ReadCounterIchor": cleanReadCounterIchor,
    "SequenzaWorkflow": cleanSequenzaWorkflow,
    "WaspMapping": cleanWaspMapping,
    "PairedSVCaller": cleanPairedSVCaller
}

module.exports = { analysisDict, processDict, cleanDict }; 