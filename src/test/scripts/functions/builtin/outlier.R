#-------------------------------------------------------------
#
# Copyright 2018 Graz University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#-------------------------------------------------------------

args<-commandArgs(TRUE)
options(digits=22)
library("Matrix")
library("outliers")

X = as.matrix(readMM(paste(args[1], "A.mtx", sep="")))
Y = t(as.matrix(outlier(X, opposite=as.logical(args[2]))));
writeMM(as(Y, "CsparseMatrix"), paste(args[3], "B", sep="")); 
 