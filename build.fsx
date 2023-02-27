#I @"tools/FAKE/tools"
#r "FakeLib.dll"

open System
open System.IO
open System.Text

open Fake
open Fake.DotNetCli
open Fake.DocFxHelper
open Fake.NuGet.Install

// Variables
let configuration = "Release"
let solution = System.IO.Path.GetFullPath(string "./src/Akka.Persistence.PostgreSql.sln")

// Directories
let toolsDir = __SOURCE_DIRECTORY__ @@ "tools"
let output = __SOURCE_DIRECTORY__  @@ "bin"
let outputTests = __SOURCE_DIRECTORY__ @@ "TestResults"
let outputPerfTests = __SOURCE_DIRECTORY__ @@ "PerfResults"
let outputBinaries = output @@ "binaries"
let outputNuGet = output @@ "nuget"
let outputBinariesNet47 = outputBinaries @@ "net471"
let outputBinariesNetStandard = outputBinaries @@ "netstandard2.0"

// Read release notes and version
let buildNumber = environVarOrDefault "BUILD_NUMBER" "0"
let preReleaseVersionSuffix = "beta" + (if (not (buildNumber = "0")) then (buildNumber) else DateTime.UtcNow.Ticks.ToString())

let releaseNotes =
    File.ReadLines (__SOURCE_DIRECTORY__ @@ "RELEASE_NOTES.md")
    |> ReleaseNotesHelper.parseReleaseNotes

let versionFromReleaseNotes =
    match releaseNotes.SemVer.PreRelease with
    | Some r -> r.Origin
    | None -> ""

let versionSuffix =
    match (getBuildParam "nugetprerelease") with
    | "dev" -> preReleaseVersionSuffix
    | "" -> versionFromReleaseNotes
    | str -> str

// Configuration values for tests
let testNetFrameworkVersion = "net471"
let testNetCoreVersion = "netcoreapp3.1"
let testNetVersion = "net5.0"
    
printfn "Assembly version: %s\nNuget version; %s\n" releaseNotes.AssemblyVersion releaseNotes.NugetVersion

//--------------------------------------------------------------------------------
// Clean build results
//--------------------------------------------------------------------------------

Target "Clean" (fun _ ->
    ActivateFinalTarget "KillCreatedProcesses"

    CleanDir output
    CleanDir outputTests
    CleanDir outputPerfTests
    CleanDir outputBinaries
    CleanDir outputNuGet
    CleanDir outputBinariesNet47
    CleanDir outputBinariesNetStandard
    CleanDir "docs/_site"

    CleanDirs !! "./**/bin"
    CleanDirs !! "./**/obj"
)

//--------------------------------------------------------------------------------
// Restore packages
//--------------------------------------------------------------------------------

Target "RestorePackages" (fun _ ->
    DotNetCli.Restore
        (fun p -> 
            { p with
                Project = solution
                NoCache = true })
)

//--------------------------------------------------------------------------------
// Generate AssemblyInfo files with the version for release notes 
//--------------------------------------------------------------------------------

Target "AssemblyInfo" (fun _ ->
    XmlPokeInnerText "./src/Directory.Build.props" "//Project/PropertyGroup/VersionPrefix" releaseNotes.AssemblyVersion    
    XmlPokeInnerText "./src/Directory.Build.props" "//Project/PropertyGroup/PackageReleaseNotes" (releaseNotes.Notes |> String.concat "\n")
)

//--------------------------------------------------------------------------------
// Build the solution
//--------------------------------------------------------------------------------

Target "Build" (fun _ ->
    let additionalArgs = if versionSuffix.Length > 0 then [sprintf "/p:VersionSuffix=%s" versionSuffix] else []  

    let projects = !! "./**/*.csproj"

    let runSingleProject project =
        DotNetCli.Build
            (fun p -> 
                { p with
                    Project = project
                    Configuration = configuration 
                    AdditionalArgs = additionalArgs })

    projects |> Seq.iter (runSingleProject)
)

//--------------------------------------------------------------------------------
// Run tests
//--------------------------------------------------------------------------------

type Runtime =
    | NetCore
    | Net
    | NetFramework

let getTestAssembly runtime project =
    let assemblyPath = match runtime with
                        | NetCore -> !! ("src" @@ "**" @@ "bin" @@ "Release" @@ testNetCoreVersion @@ fileNameWithoutExt project + ".dll")
                        | NetFramework -> !! ("src" @@ "**" @@ "bin" @@ "Release" @@ testNetFrameworkVersion @@ fileNameWithoutExt project + ".dll")
                        | Net -> !! ("src" @@ "**" @@ "bin" @@ "Release" @@ testNetVersion @@ fileNameWithoutExt project + ".dll")

    if Seq.isEmpty assemblyPath then
        None
    else
        Some (assemblyPath |> Seq.head)

module internal ResultHandling =
    let (|OK|Failure|) = function
        | 0 -> OK
        | x -> Failure x

    let buildErrorMessage = function
        | OK -> None
        | Failure errorCode ->
            Some (sprintf "xUnit2 reported an error (Error Code %d)" errorCode)

    let failBuildWithMessage = function
        | DontFailBuild -> traceError
        | _ -> (fun m -> raise(FailedTestsException m))

    let failBuildIfXUnitReportedError errorLevel =
        buildErrorMessage
        >> Option.iter (failBuildWithMessage errorLevel)

Target "RunTests" <| fun _ ->
    let projects =
        match (isWindows) with
            | true -> !! "./src/**/*.Tests.*sproj"
            | _ -> !! "./src/**/*.Tests.*sproj" // if you need to filter specs for Linux vs. Windows, do it here

    ensureDirectory outputTests

    let runSingleProject project =
        let arguments =
            (sprintf "test -c Release --no-build --logger:trx --logger:\"console;verbosity=normal\" --framework %s --results-directory \"%s\" -- -parallel none" testNetFrameworkVersion outputTests)

        let result = ExecProcess(fun info ->
            info.FileName <- "dotnet"
            info.WorkingDirectory <- (Directory.GetParent project).FullName
            info.Arguments <- arguments) (TimeSpan.FromMinutes 30.0)

        ResultHandling.failBuildIfXUnitReportedError TestRunnerErrorLevel.Error result

    CreateDir outputTests
    projects |> Seq.iter (runSingleProject)

Target "RunTestsNetCore" <| fun _ ->
    let projects =
        match (isWindows) with
            | true -> !! "./src/**/*.Tests.*sproj"
            | _ -> !! "./src/**/*.Tests.*sproj" // if you need to filter specs for Linux vs. Windows, do it here

    ensureDirectory outputTests

    let runSingleProject project =
        let arguments =
            (sprintf "test -c Release --no-build --logger:trx --logger:\"console;verbosity=normal\" --framework %s --results-directory \"%s\" -- -parallel none" testNetCoreVersion outputTests)

        let result = ExecProcess(fun info ->
            info.FileName <- "dotnet"
            info.WorkingDirectory <- (Directory.GetParent project).FullName
            info.Arguments <- arguments) (TimeSpan.FromMinutes 30.0)

        ResultHandling.failBuildIfXUnitReportedError TestRunnerErrorLevel.Error result

    CreateDir outputTests
    projects |> Seq.iter (runSingleProject)

//--------------------------------------------------------------------------------
// Nuget targets 
//--------------------------------------------------------------------------------

Target "CreateNuget" (fun _ ->    
    CreateDir outputNuGet // need this to stop Azure pipelines copy stage from error-ing out
    let projects = !! "src/**/*.csproj" 
                   -- "src/**/*Tests.csproj" // Don't publish unit tests
                   -- "src/**/*Tests*.csproj"

    let runSingleProject project =
        DotNetCli.Pack
            (fun p -> 
                { p with
                    Project = project
                    Configuration = configuration
                    AdditionalArgs = ["--include-symbols"]
                    VersionSuffix = versionSuffix
                    OutputPath = "\"" + outputNuGet + "\"" })

    projects |> Seq.iter (runSingleProject)
)

Target "PublishNuget" (fun _ ->

    let projects = !! "./bin/nuget/*.nupkg" -- "./bin/nuget/*.symbols.nupkg"
    let apiKey = getBuildParamOrDefault "nugetkey" ""
    let source = getBuildParamOrDefault "nugetpublishurl" ""
    let symbolSource = getBuildParamOrDefault "symbolspublishurl" ""
    let shouldPublishSymbolsPackages = not (symbolSource = "")

    if (not (source = "") && not (apiKey = "") && shouldPublishSymbolsPackages) then
        let runSingleProject project =
            DotNetCli.RunCommand
                (fun p -> 
                    { p with 
                        TimeOut = TimeSpan.FromMinutes 10. })
                (sprintf "nuget push %s --api-key %s --source %s --symbol-source %s" project apiKey source symbolSource)

        projects |> Seq.iter (runSingleProject)
    else if (not (source = "") && not (apiKey = "") && not shouldPublishSymbolsPackages) then
        let runSingleProject project =
            DotNetCli.RunCommand
                (fun p -> 
                    { p with 
                        TimeOut = TimeSpan.FromMinutes 10. })
                (sprintf "nuget push %s --api-key %s --source %s" project apiKey source)

        projects |> Seq.iter (runSingleProject)
)

FinalTarget "KillCreatedProcesses" (fun _ ->
    log "Shutting down dotnet build-server"
    let result = ExecProcess(fun info ->
            info.FileName <- "dotnet"
            info.WorkingDirectory <- __SOURCE_DIRECTORY__
            info.Arguments <- "build-server shutdown") (System.TimeSpan.FromMinutes 2.0)
    if result <> 0 then failwithf "dotnet build-server shutdown failed"
)

//--------------------------------------------------------------------------------
// Help 
//--------------------------------------------------------------------------------

Target "Help" <| fun _ ->
    List.iter printfn [
      "usage:"
      "build [target]"
      ""
      " Targets for building:"
      " * Build      Builds"
      " * Nuget      Create and optionally publish nugets packages"
      " * RunTests   Runs tests"
      " * All        Builds, run tests, creates and optionally publish nuget packages"
      ""
      " Other Targets"
      " * Help       Display this help" 
      " * HelpNuget  Display help about creating and pushing nuget packages" 
      " * HelpDocs   Display help about creating and pushing API docs" 
      ""]

Target "HelpNuget" <| fun _ ->
    List.iter printfn [
      "usage: "
      "build Nuget [nugetkey=<key> [nugetpublishurl=<url>]] "
      "            [symbolskey=<key> symbolspublishurl=<url>] "
      "            [nugetprerelease=<prefix>]"
      ""
      "Arguments for Nuget target:"
      "   nugetprerelease=<prefix>   Creates a pre-release package."
      "                              The version will be version-prefix<date>"
      "                              Example: nugetprerelease=dev =>"
      "                                       0.6.3-dev1408191917"
      ""
      "In order to publish a nuget package, keys must be specified."
      "If a key is not specified the nuget packages will only be created on disk"
      "After a build you can find them in bin/nuget"
      ""
      "For pushing nuget packages to nuget.org and symbols to symbolsource.org"
      "you need to specify nugetkey=<key>"
      "   build Nuget nugetKey=<key for nuget.org>"
      ""
      "For pushing the ordinary nuget packages to another place than nuget.org specify the url"
      "  nugetkey=<key>  nugetpublishurl=<url>  "
      ""
      "For pushing symbols packages specify:"
      "  symbolskey=<key>  symbolspublishurl=<url> "
      ""
      "Examples:"
      "  build Nuget                      Build nuget packages to the bin/nuget folder"
      ""
      "  build Nuget nugetprerelease=dev  Build pre-release nuget packages"
      ""
      "  build Nuget nugetkey=123         Build and publish to nuget.org and symbolsource.org"
      ""
      "  build Nuget nugetprerelease=dev nugetkey=123 nugetpublishurl=http://abc"
      "              symbolskey=456 symbolspublishurl=http://xyz"
      "                                   Build and publish pre-release nuget packages to http://abc"
      "                                   and symbols packages to http://xyz"
      ""]

Target "HelpDocs" <| fun _ ->
    List.iter printfn [
      "usage: "
      "build Docs"
      "Just builds the API docs for Akka.NET locally. Does not attempt to publish."
      ""
      "build PublishDocs azureKey=<key> "
      "                  azureUrl=<url> "
      "                 [unstable=true]"
      ""
      "Arguments for PublishDocs target:"
      "   azureKey=<key>             Azure blob storage key."
      "                              Used to authenticate to the storage account."
      ""
      "   azureUrl=<url>             Base URL for Azure storage container."
      "                              FAKE will automatically set container"
      "                              names based on build parameters."
      ""
      "   [unstable=true]            Indicates that we'll publish to an Azure"
      "                              container named 'unstable'. If this param"
      "                              is not present we'll publish to containers"
      "                              'stable' and the 'release.version'"
      ""
      "In order to publish documentation all of these values must be provided."
      "Examples:"
      "  build PublishDocs azureKey=1s9HSAHA+..."
      "                    azureUrl=http://fooaccount.blob.core.windows.net/docs"
      "                                   Build and publish docs to http://fooaccount.blob.core.windows.net/docs/stable"
      "                                   and http://fooaccount.blob.core.windows.net/docs/{release.version}"
      ""
      "  build PublishDocs azureKey=1s9HSAHA+..."
      "                    azureUrl=http://fooaccount.blob.core.windows.net/docs"
      "                    unstable=true"
      "                                   Build and publish docs to http://fooaccount.blob.core.windows.net/docs/unstable"
      ""]

//--------------------------------------------------------------------------------
//  Target dependencies
//--------------------------------------------------------------------------------

Target "BuildRelease" DoNothing
Target "All" DoNothing
Target "Nuget" DoNothing

// build dependencies
"Clean" ==> "AssemblyInfo" ==> "Build"
"Build" ==> "BuildRelease"

// test dependencies
"Build" ==> "RunTests"
"Build" ==> "RunTestsNetCore"

// nuget dependencies
"BuildRelease" ==> "CreateNuget"
"CreateNuget" ==> "PublishNuget" ==> "Nuget"

"BuildRelease" ==> "All"
"RunTests" ==> "All"
"RunTestsNetCore" ==> "All"
"Nuget" ==> "All"

RunTargetOrDefault "Help"
