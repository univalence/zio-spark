"use strict";(self.webpackChunkwebsite=self.webpackChunkwebsite||[]).push([[470],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>f});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),u=c(n),m=r,f=u["".concat(l,".").concat(m)]||u[m]||d[m]||o;return n?a.createElement(f,s(s({ref:t},p),{},{components:n})):a.createElement(f,s({ref:t},p))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,s=new Array(o);s[0]=m;var i={};for(var l in t)hasOwnProperty.call(t,l)&&(i[l]=t[l]);i.originalType=e,i[u]="string"==typeof e?e:r,s[1]=i;for(var c=2;c<o;c++)s[c]=n[c];return a.createElement.apply(null,s)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},4326:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>d,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var a=n(7462),r=(n(7294),n(3905));const o={slug:"/code-examples",sidebar_position:7},s="Code Examples",i={unversionedId:"codeSnipets",id:"codeSnipets",title:"Code Examples",description:"Seq to Dataset",source:"@site/../docs/codeSnipets.md",sourceDirName:".",slug:"/code-examples",permalink:"/zio-spark/code-examples",draft:!1,editUrl:"https://github.com/univalence/zio-spark/edit/master/docs/../docs/codeSnipets.md",tags:[],version:"current",sidebarPosition:7,frontMatter:{slug:"/code-examples",sidebar_position:7},sidebar:"tutorialSidebar",previous:{title:"FAQ",permalink:"/zio-spark/faq"},next:{title:"Versioning process choice",permalink:"/zio-spark/adrs/choose-versioning-process"}},l={},c=[{value:"Seq to Dataset",id:"seq-to-dataset",level:2},{value:"Manage Error on Loads",id:"manage-error-on-loads",level:2},{value:"Manage Analysis Errors",id:"manage-analysis-errors",level:2},{value:"Cancellable Effects + Caching",id:"cancellable-effects--caching",level:2}],p={toc:c},u="wrapper";function d(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"code-examples"},"Code Examples"),(0,r.kt)("h2",{id:"seq-to-dataset"},"Seq to Dataset"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'\n//don\'t need to import ss.implicits anymore\nimport zio.spark.sql.implicits._\n\n//implicits on sequence\nval createPersonDs: URIO[SparkSession, Dataset[Person]] = Seq(Person("toto", 13)).toDS\n\n')),(0,r.kt)("h2",{id:"manage-error-on-loads"},"Manage Error on Loads"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'\n//use of ZIO + effects composition to load a file or another if it doesn\'t work\nval input: ZIO[SparkSession, Throwable, DataFrame] = SparkSession.read.csv("path1") orElse SparkSession.read.csv("path2")\n\n')),(0,r.kt)("h2",{id:"manage-analysis-errors"},"Manage Analysis Errors"),(0,r.kt)("p",null,"Analysis erros, are error returned by Spark (",(0,r.kt)("inlineCode",{parentName:"p"},"org.apache.spark.sql.AnalysisException"),") when something wrong happen building\nthe data transformation. For example, if you select a field that do not exist, of it you try to cast ",(0,r.kt)("inlineCode",{parentName:"p"},"as")," a type, for\nthe Dataframe schema doesn't match."),(0,r.kt)("p",null,"In zio-spark, those potential errors are isolated, you can either : "),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"recover from them"),(0,r.kt)("li",{parentName:"ul"},"throws directly (as would apache spark do)")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'import zio.spark.sql.implicits._ //if not done already that the beginning of the file\n\n//explicit error management + encoder on Person\ndef process1(input: DataFrame): Task[Long] = input.as[Person] match {\n  case TryAnalysis.Failure(_) => input.filter("age >= 18").getOrThrow.count\n  case TryAnalysis.Success(ds) => ds.count\n}\n\n//less error management\ndef process2(input: DataFrame) = {\n  import zio.spark.sql.syntax.throwsAnalysisException._\n\n  val selectPerson:Dataset[Person] = input.as[Person]\n  selectPerson.filter(_.isAdult).count\n\n}\n')),(0,r.kt)("h2",{id:"cancellable-effects--caching"},"Cancellable Effects + Caching"),(0,r.kt)("p",null,"Caching mechanism is from Spark, it's effecful, you need to call it before doing a computation to mark the dataset as something\nto be cached. CancellableEffect is an experimental feature from zio-spark, that allow to propagate an Interrupt to stop the job. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"\n//cache, cancellable\ndef improveProcess(input: DataFrame): ZIO[SparkSession, Throwable, Long] =\n  for {\n    cached <- input.cache\n    p = CancellableEffect.makeItCancellable(process(cached))\n    //if the first job is stale, it launches a second job. When on of the jobs finish, it stops the remaining job\n    count <- p.race(p.delay(10.seconds))\n  } yield count\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"")))}d.isMDXComponent=!0}}]);